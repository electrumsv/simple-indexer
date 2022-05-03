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
from typing import List, Optional


RPC_URI = "http://rpcuser:rpcpassword@127.0.0.1:18332"


def submit_block(block_bytes: bytes) -> None:
    block_bytes_hex = block_bytes.hex()
    payload = json.dumps({"jsonrpc": "2.0", "method": "submitblock", "params": [ block_bytes_hex ],
        "id": 0})
    result = requests.post(f"{RPC_URI}", data=payload, timeout=10.0)
    result.raise_for_status()


def import_blocks(output_dir_path: str, to_height: Optional[int]) -> None:
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

    for height, header_hash_hex in enumerate(header_hash_hexs):
        if to_height and height > to_height:
            break
        print(f"Uploading block {height} {header_hash_hex[:6]}")
        block_file_path = os.path.join(output_dir_path, header_hash_hex)
        with open(block_file_path, "rb") as bf:
            block_bytes = bf.read()
            submit_block(block_bytes)


def main() -> None:
    if len(sys.argv) < 2:
        print(f"{sys.argv[0]} <directory path> [<to_height>]")
        sys.exit(1)

    output_dir_path = os.path.realpath(sys.argv[1])
    if not os.path.exists(output_dir_path) or not os.path.isdir(output_dir_path):
        print(f"Directory does not exist: {sys.argv[1]}")
        sys.exit(1)

    to_height = None
    if len(sys.argv) == 3 and sys.argv[2]:
        to_height_text = sys.argv[2]
        if not to_height_text.isdigit():
            print("to_height optional argument is not a number")
            sys.exit(1)
        to_height = int(to_height_text)

    import_blocks(output_dir_path, to_height)

    sys.exit(0)


if __name__ == "__main__":
    main()

