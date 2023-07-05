"""
This file is almost a direct copy from the ElectrumSV project at
https://github.com/electrumsv/electrumsv in electrumsv/cached_headers.py
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import cast

import bitcoinx
from bitcoinx import Headers, Network


# A reference to this cursor must be maintained and passed to the Headers.unpersisted_headers
# function in order to determine which newly appended headers still need to be appended
# to disc
HeaderPersistenceCursor = dict[bitcoinx.Chain, int]


logger = logging.getLogger("cached-headers")


def write_cached_headers(headers: Headers, cursor: HeaderPersistenceCursor,
        headers_file_path: Path) -> HeaderPersistenceCursor:
    with open(headers_file_path, "ab") as hf:
        hf.write(headers.unpersisted_headers(cursor))
    return cast(HeaderPersistenceCursor, headers.cursor())


def read_cached_headers(coin: Network, file_path: str) -> tuple[Headers, HeaderPersistenceCursor]:
    # See app_state._migrate. A 'headers3' file should always be present on mainnet
    if coin.name == 'mainnet':
        assert os.path.exists(file_path)
    elif not os.path.exists(file_path):
        open(file_path, 'wb').close()
    logger.debug("New headers storage file: %s found", file_path)
    with open(file_path, "rb") as f:
        raw_headers = f.read()
    headers = Headers(coin)
    cursor = headers.connect_many(raw_headers, check_work=False)
    return headers, cursor
