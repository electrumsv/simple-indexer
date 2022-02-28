"""
Copyright(c) 2021, 2022 Bitcoin Association.
Distributed under the Open BSV software license, see the accompanying file LICENSE

Note on typing
--------------

Write database functions are run in the SQLite writer thread using the helper functions from
the `sqlite_database` package, and because of this have to follow the pattern where the database
is an optional last argument.

    ```
    def create_account(public_key_bytes: bytes, forced_api_key: Optional[str] = None,
            db: Optional[sqlite3.Connection]=None) -> tuple[int, str]:
        assert db is not None and isinstance(db, sqlite3.Connection)
        ...
    ```

This is not required for reading functions as they should run generally run inline unless they
are long running, in which case they should be handed off to a worker thread.
"""

from __future__ import annotations
import logging
import os
import sqlite3
import threading
from typing import cast, Generator, NamedTuple, Optional, Union

from bitcoinx import hash_to_hex_str
from electrumsv_database.sqlite import read_rows_by_id, read_rows_by_ids, \
    replace_db_context_with_connection

from .constants import MAX_UINT32
from .types import OutpointType, OutputSpendRow, RestorationFilterJSONResponse, \
    RestorationFilterResult


logger = logging.getLogger("sqlite-database")
mined_tx_hashes_table_lock = threading.RLock()


def setup(db: Optional[sqlite3.Connection]=None) -> None:
    assert db is not None and isinstance(db, sqlite3.Connection)
    if int(os.getenv('SIMPLE_INDEX_RESET', "1")):
        drop_tables(db)
    create_tables(db)

def create_tables(db: sqlite3.Connection) -> None:
    create_confirmed_tx_table(db)
    create_txos_table(db)
    create_mempool_tx_table(db)
    create_inputs_table(db)
    create_pushdata_table(db)
    create_blocks_table(db)

def drop_tables(db: sqlite3.Connection) -> None:
    drop_confirmed_tx_table(db)
    drop_txos_table(db)
    drop_inputs_table(db)
    drop_pushdata_table(db)
    drop_mempool_tx_table(db)
    drop_raw_blocks_table(db)


def create_blocks_table(db: sqlite3.Connection) -> None:
    db.execute("""
        CREATE TABLE IF NOT EXISTS blocks (
            block_hash BINARY(32),
            block_height INTEGER,
            raw_block BLOB
        )
    """)
    db.execute("CREATE UNIQUE INDEX IF NOT EXISTS block_hash_idx ON blocks (block_hash)")

def drop_raw_blocks_table(db: sqlite3.Connection) -> None:
    db.execute("DROP TABLE IF EXISTS blocks")

def create_confirmed_tx_table(db: sqlite3.Connection) -> None:
    db.execute("""
        CREATE TABLE IF NOT EXISTS confirmed_transactions (
            tx_hash BINARY(32),
            block_hash BINARY(32),
            tx_position INTEGER,
            rawtx BLOB
        )
    """)
    # NOTE: The UNIQUE constraint must cover both tx_hash + block_hash so that we can record
    # the same tx on both sides of a fork
    db.execute("CREATE UNIQUE INDEX IF NOT EXISTS tx_idx ON confirmed_transactions "
        "(tx_hash, block_hash)")

def drop_confirmed_tx_table(db: sqlite3.Connection) -> None:
    db.execute("DROP TABLE IF EXISTS confirmed_transactions")

def create_mempool_tx_table(db: sqlite3.Connection) -> None:
    db.execute("""
        CREATE TABLE IF NOT EXISTS mempool_transactions (
            mp_tx_hash BINARY(32),
            mp_tx_timestamp BINARY(32),
            rawtx BLOB
        )
    """)
    db.execute("CREATE UNIQUE INDEX IF NOT EXISTS mp_tx_idx ON mempool_transactions (mp_tx_hash)")

def drop_mempool_tx_table(db: sqlite3.Connection) -> None:
    db.execute("DROP TABLE IF EXISTS mempool_transactions")

def create_txos_table(db: sqlite3.Connection) -> None:
    db.execute("""
    CREATE TABLE IF NOT EXISTS txos (
        out_tx_hash BINARY(32),
        out_idx INTEGER,
        out_value INTEGER,
        out_scriptpubkey BLOB
    )
    """)
    db.execute("CREATE UNIQUE INDEX IF NOT EXISTS txo_idx ON txos (out_tx_hash, out_idx)")

def drop_txos_table(db: sqlite3.Connection) -> None:
    db.execute("DROP TABLE IF EXISTS txos")

def create_inputs_table(db: sqlite3.Connection) -> None:
    db.execute("""
    CREATE TABLE IF NOT EXISTS inputs (
        out_tx_hash BINARY(32),
        out_idx INTEGER,
        in_tx_hash BINARY(32),
        in_idx INTEGER,
        in_scriptsig BLOB
    )
    """)
    # NOTE: For coinbases all have the same
    # out_tx_hash == '0000000000000000000000000000000000000000000000000000000000000000'
    db.execute("CREATE UNIQUE INDEX IF NOT EXISTS inputs_idx ON inputs "
        "(out_tx_hash, out_idx, in_tx_hash, in_idx)")

def drop_inputs_table(db: sqlite3.Connection) -> None:
    db.execute("DROP TABLE IF EXISTS inputs")

def create_pushdata_table(db: sqlite3.Connection) -> None:
    db.execute("""
    CREATE TABLE IF NOT EXISTS pushdata (
        pushdata_hash BINARY(32),
        tx_hash BINARY(32),
        idx INTEGER,
        ref_type SMALLINT
    )
    """)
    db.execute("CREATE UNIQUE INDEX IF NOT EXISTS pushdata_idx ON pushdata "
        "(pushdata_hash, tx_hash, idx, ref_type)")

def drop_pushdata_table(db: sqlite3.Connection) -> None:
    db.execute("DROP TABLE IF EXISTS pushdata")

# ----- Database operations ----- #
def insert_tx_rows(tx_rows: list[tuple[bytes, bytes, int, bytes]],
        db: Optional[sqlite3.Connection]=None) -> None:
    assert db is not None and isinstance(db, sqlite3.Connection)
    db.executemany("""
        INSERT INTO confirmed_transactions (tx_hash, block_hash, tx_position, rawtx)
        VALUES (?, ?, ?, ?)
    """, tx_rows)

def insert_mempool_tx_rows(tx_rows: list[tuple[bytes, float, bytes]],
        db: Optional[sqlite3.Connection]=None) -> None:
    assert db is not None and isinstance(db, sqlite3.Connection)
    db.executemany("""
        INSERT INTO mempool_transactions (mp_tx_hash, mp_tx_timestamp, rawtx)
        VALUES (?, ?, ?)
    """, tx_rows)

def insert_txo_rows(txo_rows: list[tuple[bytes, int, int, bytes]],
        db: Optional[sqlite3.Connection]=None) -> None:
    assert db is not None and isinstance(db, sqlite3.Connection)
    db.executemany("""
        INSERT INTO txos (out_tx_hash, out_idx, out_value, out_scriptpubkey)
        VALUES (?, ?, ?, ?)
    """, txo_rows)

def insert_input_rows(input_rows: list[tuple[bytes, int, bytes, int, bytes]],
        db: Optional[sqlite3.Connection]=None) -> None:
    assert db is not None and isinstance(db, sqlite3.Connection)
    db.executemany("""
        INSERT INTO inputs (out_tx_hash, out_idx, in_tx_hash, in_idx, in_scriptsig)
        VALUES (?, ?, ?, ?, ?)
    """, input_rows)

def insert_pushdata_rows(pushdata_rows: list[tuple[bytes, bytes, int, int]],
        db: Optional[sqlite3.Connection]=None) -> None:
    assert db is not None and isinstance(db, sqlite3.Connection)
    db.executemany("""
        INSERT INTO pushdata (pushdata_hash, tx_hash, idx, ref_type)
        VALUES (?, ?, ?, ?)
    """, pushdata_rows)

def create_temp_block_tx_hashes_table(db: sqlite3.Connection) -> None:
    """Used to join on mempool"""
    db.execute("""
        CREATE TEMPORARY TABLE IF NOT EXISTS mined_tx_hashes (
            tx_hash BINARY(32)
        )
    """)
    db.execute("CREATE INDEX IF NOT EXISTS temp_table_tx_idx ON mined_tx_hashes (tx_hash)")

def drop_temp_block_hashes_table(db: sqlite3.Connection) -> None:
    db.execute("DROP TABLE IF EXISTS mined_tx_hashes")

def get_transaction_mempool(tx_hash: bytes, db: Optional[sqlite3.Connection]=None) \
        -> Optional[bytes]:
    assert db is not None and isinstance(db, sqlite3.Connection)
    sql = "SELECT rawtx FROM mempool_transactions WHERE mp_tx_hash = ?"
    result = db.execute(sql, (tx_hash,)).fetchall()
    if len(result) == 0:
        return None
    return cast(bytes, result[0][0])

@replace_db_context_with_connection
def get_transaction(db: sqlite3.Connection, tx_hash: bytes) -> Optional[bytes]:
    assert db is not None and isinstance(db, sqlite3.Connection)
    sql = "SELECT rawtx FROM confirmed_transactions WHERE tx_hash = ?"
    result = db.execute(sql, (tx_hash,)).fetchall()
    if len(result) == 0:
        return get_transaction_mempool(tx_hash, db)
    return cast(bytes, result[0][0])

@replace_db_context_with_connection
def get_block_hash_for_tx(db: sqlite3.Connection, tx_hash: bytes) -> Optional[bytes]:
    assert db is not None and isinstance(db, sqlite3.Connection)
    sql = "SELECT block_hash FROM confirmed_transactions WHERE tx_hash = ?"
    result = db.execute(sql, (tx_hash,)).fetchall()
    if len(result) == 0:
        return None
    return cast(bytes, result[0][0])

@replace_db_context_with_connection
def get_matching_mempool_txids(db: sqlite3.Connection, tx_hashes: set[bytes]) \
        -> set[bytes]:
    assert db is not None and isinstance(db, sqlite3.Connection)
    with mined_tx_hashes_table_lock:
        create_temp_block_tx_hashes_table(db)

        sql = "INSERT INTO mined_tx_hashes (tx_hash) VALUES (?)"
        db.executemany(sql, [ (tx_hash,) for tx_hash in tx_hashes ])

        # Run SELECT query to find txs that have already been processed
        sql = """
            SELECT mp_tx_hash
            FROM mempool_transactions
            JOIN mined_tx_hashes ON tx_hash = mp_tx_hash
        """

        processed_tx_hashes: set[bytes] = { row[0] for row in db.execute(sql).fetchall() }
        drop_temp_block_hashes_table(db)
        return processed_tx_hashes

def invalidate_mempool(tx_hashes: set[bytes], db: Optional[sqlite3.Connection]=None) -> None:
    assert db is not None and isinstance(db, sqlite3.Connection)
    sql = "DELETE FROM mempool_transactions WHERE mp_tx_hash = ?"
    db.executemany(sql, [ (tx_hash,) for tx_hash in tx_hashes ])

def insert_block_row(block_hash: bytes, height: int, raw_block: bytes,
        db: Optional[sqlite3.Connection]=None) -> None:
    assert db is not None and isinstance(db, sqlite3.Connection)
    db.execute("INSERT INTO blocks VALUES (?, ?, ?)", (block_hash, height, raw_block))

def get_pushdata_match_flag(ref_type: int) -> int:
    if ref_type == 0:
        return 1 << 0
    if ref_type == 1:
        return 1 << 1
    raise NotImplementedError


class FilterMatchRow(NamedTuple):
    pushdata_hash: bytes
    tx_hash: bytes
    output_index: int
    ref_type: int
    input_tx_hash: Optional[bytes]
    input_index: int

@replace_db_context_with_connection
def get_pushdata_filter_matches(db: sqlite3.Connection, pushdata_hashes: list[bytes],
        json: bool=True) \
        -> Generator[Union[RestorationFilterJSONResponse, RestorationFilterResult], None, None]:
    assert db is not None and isinstance(db, sqlite3.Connection)
    # The matched transaction id will either be:
    # - Where the pushdata is found in an input script (flags & 1<<1 != 0).
    #   - This means the index value is the index of a transaction input.
    #   - There will never be any spent transaction id or index for this match.
    # - Where the pushdata is found in an output script (flags & 1<<0 != 0).
    #   - This means the index value is the index of a transaction output.
    #   - There should always be any spent transaction id or index for this match.
    #     - The restoration index is intended to only server mined transactions and not
    #       mempool
    sql = """
        SELECT PD.pushdata_hash, PD.tx_hash, PD.idx, PD.ref_type, IT.in_tx_hash, IT.in_idx
        FROM pushdata PD
        LEFT JOIN inputs IT ON PD.tx_hash=IT.out_tx_hash AND PD.idx=IT.out_idx AND PD.ref_type=0
        INNER JOIN confirmed_transactions CT ON PD.tx_hash = CT.tx_hash
        WHERE PD.pushdata_hash IN ({})
    """
    for row in read_rows_by_id(FilterMatchRow, db, sql, [], pushdata_hashes):
        if json:
            json_match: RestorationFilterJSONResponse = {
                "pushDataHashHex": row.pushdata_hash.hex(),
                "lockingTransactionId": hash_to_hex_str(row.tx_hash),
                "lockingTransactionIndex": row.output_index,
                "flags": get_pushdata_match_flag(row.ref_type),
                "unlockingTransactionId": "00"*32 if row.input_tx_hash is None \
                    else hash_to_hex_str(row.input_tx_hash),
                "unlockingInputIndex": MAX_UINT32 if row.input_index is None else row.input_index,
            }
            yield json_match
        else:
            yield RestorationFilterResult(
                flags=get_pushdata_match_flag(row.ref_type),
                push_data_hash=row.pushdata_hash,
                locking_transaction_hash=row.tx_hash,
                locking_output_index=row.output_index,
                unlocking_transaction_hash=bytes(32) if row.input_tx_hash is None \
                    else row.input_tx_hash,
                unlocking_input_index=MAX_UINT32 if row.input_index is None else row.input_index
            )

@replace_db_context_with_connection
def get_spent_outpoints(db: sqlite3.Connection, entries: list[OutpointType]) \
        -> list[OutputSpendRow]:
    sql = """
        SELECT I.out_tx_hash, I.out_idx, I.in_tx_hash, I.in_idx, CT.block_hash
        FROM inputs I
        LEFT JOIN confirmed_transactions CT ON CT.tx_hash = I.in_tx_hash
    """
    sql_condition = "I.out_tx_hash=? AND I.out_idx=?"
    return read_rows_by_ids(OutputSpendRow, db, sql, sql_condition, [], entries)
