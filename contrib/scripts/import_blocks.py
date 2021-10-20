"""
Import the blocks exported to a local directory into a running node.

> py import_blocks.py <local directory>

The directory should contain a `headers.txt` file and an additional file for each block named
as the hash for the given block.
"""

import json
import os
import requests
import sys
from typing import cast, List, TypedDict

from bitcoinx import hash_to_hex_str, double_sha256

RPC_URI = "http://rpcuser:rpcpassword@127.0.0.1:18332"


def submit_block(block_bytes: bytes) -> None:
    block_bytes_hex = block_bytes.hex()
    payload = json.dumps({"jsonrpc": "2.0", "method": "submitblock", "params": [ block_bytes_hex ], "id": 0})
    result = requests.post(f"{RPC_URI}", data=payload, timeout=10.0)
    result.raise_for_status()


def main() -> None:
    if len(sys.argv) != 2:
        print(f"{sys.argv[0]} <directory path>")
        sys.exit(1)

    output_dir_path = os.path.realpath(sys.argv[1])
    if not os.path.exists(output_dir_path) or not os.path.isdir(output_dir_path):
        print(f"Directory does not exist: {sys.argv[1]}")
        sys.exit(1)

    headers_file_path = os.path.join(output_dir_path, "headers.txt")
    if not os.path.exists(headers_file_path):
        print(f"Directory does not look like a blockchain export: {sys.argv[1]}")
        sys.exit(1)

    header_hash_hexs: List[str] = []
    with open(headers_file_path, "r") as hf:
        i = 0
        while 1:
            header_hash_hex = hf.readline().strip()
            if not header_hash_hex:
                break
            block_file_path = os.path.join(output_dir_path, header_hash_hex)
            if not os.path.exists(block_file_path):
                print(f"Missing block {i} {header_hash_hex[:6]}")
                sys.exit(1)
            header_hash_hexs.append(header_hash_hex)
            i += 1

    for i, header_hash_hex in enumerate(header_hash_hexs):
        print(f"Uploading block {i} {header_hash_hex[:6]}")
        block_file_path = os.path.join(output_dir_path, header_hash_hex)
        with open(block_file_path, "rb") as bf:
            block_bytes = bf.read()
            submit_block(block_bytes)

    sys.exit(0)


if __name__ == "__main__":
    main()

