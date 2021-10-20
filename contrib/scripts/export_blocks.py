"""
Export all the blocks in the best chain in the node.

> mkdir blockchain
> py contrib\\scripts\\export_blocks.py blockchain

This will create a `blockchain\\blockchain_<height>_<shorthash>` sub-directory, and put a
text `headers.txt` file with each header on a new-line in order for the chain, and each block in
a binary file under the name for it's block hash.

"""

import io
import os
import requests
import sys
from typing import cast, List, TypedDict

from bitcoinx import hash_to_hex_str, double_sha256

REST_URI = "http://127.0.0.1:18332/rest"
GENESIS_HASH_HEX = "0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206"


class BlockchainInfo(TypedDict):
    chain: str
    blocks: int
    headers: int
    bestblockhash: str
    difficulty: float
    mediantime: int
    verificationprogress: int
    chainwork: str
    pruned: bool
    # softforks: ... who cares


def get_blockchain_info() -> BlockchainInfo:
    response = requests.get(f"{REST_URI}/chaininfo.json")
    return cast(BlockchainInfo, response.json())


def get_header_bytes(height: int) -> bytes:
    # Height is a 0-based index up to the best block. This is count of the blocks after Genesis.
    # We want to include Genesis, as it is our starting block.
    block_count = height + 1
    response = requests.get(f"{REST_URI}/headers/{block_count}/{GENESIS_HASH_HEX}.bin")
    return response.content


def get_block_bytes(hash_hex: str) -> bytes:
    response = requests.get(f"{REST_URI}/block/{hash_hex}.bin")
    return response.content


def main() -> None:
    if len(sys.argv) != 2:
        print(f"{sys.argv[0]} <directory path>")
        sys.exit(1)

    output_dir_path = os.path.realpath(sys.argv[1])
    if not os.path.exists(output_dir_path) or not os.path.isdir(output_dir_path):
        print(f"Directory does not exist: {sys.argv[1]}")
        sys.exit(1)

    try:
        info = get_blockchain_info()
    except requests.exceptions.ConnectionError:
        print("Connection error.")
        sys.exit(1)

    # This is the number of blocks above the genesis block.
    block_count_excluding_genesis = info["blocks"]
    if block_count_excluding_genesis == 0:
        print("The blockchain is empty.")
        sys.exit(1)

    height = block_count_excluding_genesis

    print(f"Blockchain height: {height}")
    try:
        headers_bytes = get_header_bytes(height)
    except requests.exceptions.ConnectionError:
        print("Connection error.")
        sys.exit(1)

    # Genesis block plus height amount of additional blocks above it.
    if height+1 != len(headers_bytes)//80:
        print(f"Blockchain mismatch: height={height+1} does not match headers_count={len(headers_bytes)//80}")
        sys.exit(1)

    bestblockhash_hex = info["bestblockhash"]
    chain_path = os.path.join(output_dir_path, f"blockchain_{height}_{bestblockhash_hex[:6]}")
    if os.path.exists(chain_path):
        print(f"Blockchain directory already exists: {chain_path}")
        sys.exit(1)

    os.makedirs(chain_path)

    header_io = io.StringIO()
    headers_view = memoryview(headers_bytes)
    header_hash_hexs: List[str] = []
    for i in range(height+1):
        header_hash = double_sha256(headers_view[i*80:i*80+80])
        header_hash_hex = hash_to_hex_str(header_hash)
        header_io.write(header_hash_hex)
        header_io.write("\n")
        header_hash_hexs.append(header_hash_hex)

    if header_hash_hexs[-1] != bestblockhash_hex:
        print(f"Tip mismatch, export code is buggy: {header_hash_hexs[-1][:6]} != {bestblockhash_hex[:6]}")
        sys.exit(1)

    for i, header_hash_hex in enumerate(header_hash_hexs):
        print(f"Fetching block {i} {header_hash_hex[:6]}")
        try:
            block_bytes = get_block_bytes(header_hash_hex)
        except requests.exceptions.ConnectionError:
            print(f"Connection error reading block {i}.")
            sys.exit(1)

        block_file_path = os.path.join(chain_path, header_hash_hex)
        with open(block_file_path, "wb") as bf:
            bf.write(block_bytes)

    headers_file_path = os.path.join(chain_path, "headers.txt")
    with open(headers_file_path, "w") as hf:
        hf.write(header_io.getvalue())

    sys.exit(0)


if __name__ == "__main__":
    main()

