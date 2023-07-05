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
import sys

if sys.platform == 'linux':
    import pysqlite3  # pylint: disable=import-error
    sqlite3.Connection = pysqlite3.Connection

import threading
from typing import Any, cast, Generator, NamedTuple, Optional, Union

from bitcoinx import hash_to_hex_str
from electrumsv_database.sqlite import (
    read_rows_by_id, read_rows_by_ids, replace_db_context_with_connection
)

from .constants import AccountFlag, MAX_UINT32, OutboundDataFlag
from .types import (
    AccountMetadata, IndexerPushdataRegistrationFlag, OutboundDataRow, OutpointType, OutputSpendRow,
    PushDataRow, RestorationFilterJSONResponse, RestorationFilterResult, TipFilterRegistrationEntry
)

logger = logging.getLogger("sqlite-database")
mined_tx_hashes_table_lock = threading.RLock()


class DatabaseError(Exception):
    pass

class DatabaseStateModifiedError(DatabaseError):
    # The database state was not as we required it to be in some way.
    pass

class DatabaseInsertConflict(DatabaseError):
    pass


def setup(db: Optional[sqlite3.Connection]=None) -> None:
    assert db is not None
    if int(os.getenv('SIMPLE_INDEX_RESET', "0")):
        drop_tables(db)
    create_tables(db)

def create_tables(db: sqlite3.Connection) -> None:
    logger.debug("creating database tables")
    create_confirmed_tx_table(db)
    create_txos_table(db)
    create_mempool_tx_table(db)
    create_inputs_table(db)
    create_accounts_table(db)
    create_pushdata_table(db)
    create_blocks_table(db)
    create_tip_filter_registrations_table(db)
    create_outbound_data_table(db)

def drop_tables(db: sqlite3.Connection) -> None:
    logger.debug("dropping database tables")
    drop_confirmed_tx_table(db)
    drop_txos_table(db)
    drop_inputs_table(db)
    drop_accounts_table(db)
    drop_pushdata_table(db)
    drop_mempool_tx_table(db)
    drop_raw_blocks_table(db)
    drop_tip_filter_registrations_table(db)
    drop_outbound_data_table(db)


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

def create_accounts_table(db: sqlite3.Connection) -> None:
    db.execute("""
    CREATE TABLE IF NOT EXISTS accounts (
        account_id                  INTEGER     PRIMARY KEY,
        external_account_id         INTEGER     NOT NULL,
        account_flags               INTEGER     NOT NULL
    )
    """)
    db.execute("CREATE UNIQUE INDEX IF NOT EXISTS accounts_external_id_idx "
        "ON accounts (external_account_id)")

def drop_accounts_table(db: sqlite3.Connection) -> None:
    db.execute("DROP TABLE IF EXISTS accounts")

def create_tip_filter_registrations_table(db: sqlite3.Connection) -> None:
    db.execute("""
    CREATE TABLE IF NOT EXISTS indexer_filtering_registrations_pushdata (
        account_id              INTEGER     NOT NULL,
        pushdata_hash           BINARY(32)  NOT NULL,
        flags                   INTEGER     NOT NULL,
        date_expires            INTEGER     NOT NULL,
        date_created            INTEGER     NOT NULL,
        FOREIGN KEY (account_id) REFERENCES Accounts (account_id)
    )
    """)
    db.execute("CREATE UNIQUE INDEX IF NOT EXISTS indexer_filtering_registrations_pushdata_idx "
        "ON indexer_filtering_registrations_pushdata (account_id, pushdata_hash)")

def drop_tip_filter_registrations_table(db: sqlite3.Connection) -> None:
    db.execute("DROP TABLE IF EXISTS tip_filter_registrations")

def create_outbound_data_table(db: sqlite3.Connection) -> None:
    db.execute("""
    CREATE TABLE IF NOT EXISTS outbound_data (
        outbound_data_id        INTEGER     PRIMARY KEY,
        outbound_data           BLOB        NOT NULL,
        outbound_data_flags     INTEGER     NOT NULL,
        date_created            INTEGER     NOT NULL,
        date_last_tried         INTEGER     NOT NULL
    )
    """)

def drop_outbound_data_table(db: sqlite3.Connection) -> None:
    db.execute("DROP TABLE IF EXISTS outbound_data")

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
    try:
        db.executemany("""
            INSERT INTO mempool_transactions (mp_tx_hash, mp_tx_timestamp, rawtx)
            VALUES (?, ?, ?)
        """, tx_rows)
    except sqlite3.IntegrityError:
        raise DatabaseInsertConflict()

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

def insert_pushdata_rows(pushdata_rows: list[PushDataRow],
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


def create_account_write(external_account_id: int, db: Optional[sqlite3.Connection]=None) -> int:
    """
    This does partial updates depending on what is in `settings`.
    """
    sql_values: tuple[Any, ...]
    assert db is not None and isinstance(db, sqlite3.Connection)
    row = db.execute("SELECT account_id FROM accounts WHERE external_account_id=?",
        (external_account_id,)).fetchone()
    if row is not None:
        return cast(int, row[0])

    sql = """
    INSERT INTO accounts (external_account_id, account_flags)
    VALUES (?, ?)
    RETURNING account_id
    """
    sql_values = (external_account_id, AccountFlag.NONE)
    row = db.execute(sql, sql_values).fetchone()
    logger.debug("Created account in indexer settings db callback %d", row[0])
    return cast(int, row[0])


@replace_db_context_with_connection
def read_account_metadata(db: sqlite3.Connection, account_ids: list[int]) \
        -> list[AccountMetadata]:
    sql = """
        SELECT account_id, external_account_id, account_flags
        FROM accounts
        WHERE account_id IN ({})
    """
    return read_rows_by_id(AccountMetadata, db, sql, [], account_ids)


@replace_db_context_with_connection
def read_account_id_for_external_account_id(db: sqlite3.Connection, external_account_id: int) \
        -> int:
    row = db.execute("SELECT account_id FROM accounts WHERE external_account_id=?",
        (external_account_id,)).fetchone()
    assert row is not None
    return cast(int, row[0])


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
def get_restoration_matches(db: sqlite3.Connection, pushdata_hashes: list[bytes], json: bool=True) \
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


def create_indexer_filtering_registrations_pushdatas_write(external_account_id: int,
        date_created: int, registration_entries: list[TipFilterRegistrationEntry],
        db: Optional[sqlite3.Connection]=None) -> bool:
    assert db is not None and isinstance(db, sqlite3.Connection)

    account_id = create_account_write(external_account_id, db)

    # We use the SQLite `OR ABORT` clause to ensure we either insert all registrations or none
    # if some are already present. This means we do not need to rely on rolling back the
    # transaction because no changes should have been made in event of conflict.
    sql = """
    INSERT OR ABORT INTO indexer_filtering_registrations_pushdata
        (account_id, pushdata_hash, flags, date_created, date_expires) VALUES (?, ?, ?, ?, ?)
    """
    insert_rows: list[tuple[int, bytes, int, int, int]] = []
    for entry in registration_entries:
        insert_rows.append((account_id, entry.pushdata_hash,
            IndexerPushdataRegistrationFlag.FINALISED, date_created, date_created +
            entry.duration_seconds))
    try:
        db.executemany(sql, insert_rows)
    except sqlite3.IntegrityError:
        logger.exception("Failed inserting filtering registrations")
        # No changes should have been made. Indicate that what was inserted was nothing.
        return False
    else:
        return True


@replace_db_context_with_connection
def read_indexer_filtering_registrations_pushdatas(db: sqlite3.Connection,
        account_id: Optional[int]=None,
        date_expires: Optional[int]=None,
        # These defaults include all rows no matter the flag value.
        expected_flags: IndexerPushdataRegistrationFlag=IndexerPushdataRegistrationFlag.NONE,
        mask: IndexerPushdataRegistrationFlag=IndexerPushdataRegistrationFlag.NONE) \
            -> list[TipFilterRegistrationEntry]:
    """
    Load the non-expired tip filter registrations from the database especially to populate the
    tip filter.
    """
    sql_values: list[Any] = [ mask, expected_flags ]
    sql = "SELECT pushdata_hash, date_expires FROM indexer_filtering_registrations_pushdata " \
        "WHERE flags&?=?"
    if account_id is not None:
        sql += " AND account_id=?"
        sql_values.append(account_id)
    if date_expires is not None:
        sql += " AND date_expires < ?"
        sql_values.append(date_expires)
    return [ TipFilterRegistrationEntry(*row) for row in db.execute(sql, sql_values).fetchall() ]


class FilterNotificationRow(NamedTuple):
    account_id: int
    pushdata_hash: bytes


@replace_db_context_with_connection
def read_indexer_filtering_registrations_for_notifications(db: sqlite3.Connection,
        pushdata_hashes: list[bytes]) -> list[FilterNotificationRow]:
    """
    These are the matches that in either a new mempool transaction or a block which were
    present (correctly or falsely) in the common cuckoo filter.
    """
    sql = "SELECT account_id, pushdata_hash FROM indexer_filtering_registrations_pushdata " \
        "WHERE (flags&?)=? AND pushdata_hash IN ({})"

    sql_values = [
        IndexerPushdataRegistrationFlag.FINALISED | IndexerPushdataRegistrationFlag.DELETING,
        IndexerPushdataRegistrationFlag.FINALISED
    ]
    return read_rows_by_id(FilterNotificationRow, db, sql, sql_values, pushdata_hashes)


def update_indexer_filtering_registrations_pushdatas_flags_write(
        external_account_id: int, pushdata_hashes: list[bytes],
        update_flags: IndexerPushdataRegistrationFlag=IndexerPushdataRegistrationFlag.NONE,
        update_mask: Optional[IndexerPushdataRegistrationFlag]=None,
        filter_flags: IndexerPushdataRegistrationFlag=IndexerPushdataRegistrationFlag.NONE,
        filter_mask: Optional[IndexerPushdataRegistrationFlag]=None,
        require_all: bool=False, db: Optional[sqlite3.Connection]=None) -> None:
    assert db is not None and isinstance(db, sqlite3.Connection)

    account_id = create_account_write(external_account_id, db)

    # Ensure that the update only affects the update flags if no mask is provided.
    final_update_mask = ~update_flags if update_mask is None else update_mask
    # Ensure that the filter only looks at the filter flags if no mask is provided.
    final_filter_mask = filter_flags if filter_mask is None else filter_mask
    sql = """
    UPDATE indexer_filtering_registrations_pushdata
    SET flags=(flags&?)|?
    WHERE account_id=? AND pushdata_hash=? AND (flags&?)=?
    """
    update_rows: list[tuple[int, int, int, bytes, int, int]] = []
    for pushdata_value in pushdata_hashes:
        update_rows.append((final_update_mask, update_flags, account_id, pushdata_value,
            final_filter_mask, filter_flags))
    cursor = db.executemany(sql, update_rows)
    if require_all and cursor.rowcount != len(pushdata_hashes):
        raise DatabaseStateModifiedError


def expire_indexer_filtering_registrations_pushdatas(date_expires: int,
        db: Optional[sqlite3.Connection]=None) -> list[bytes]:
    """
    Atomic call to locate expired registrations and to delete them. It will return the keys for
    all the rows that were deleted.

    Returns `[ (account_id, pushdata_hash), ... ]`
    Raises no known exceptions.
    """
    assert db is not None and isinstance(db, sqlite3.Connection)
    # In the case of `DELETE` statements the `RETURNING` values come from the row being deleted.
    sql_deletion = "DELETE FROM indexer_filtering_registrations_pushdata " \
        "WHERE flags&?=? AND date_expires<? " \
        "RETURNING pushdata_hash"
    expected_flags = IndexerPushdataRegistrationFlag.FINALISED
    mask_flags = IndexerPushdataRegistrationFlag.DELETING|expected_flags
    sql_values = (mask_flags, expected_flags, date_expires)
    return [ cast(bytes, row[0]) for row in db.execute(sql_deletion, sql_values).fetchall() ]


def delete_indexer_filtering_registrations_pushdatas_write(external_account_id: int,
        pushdata_hashes: list[bytes],
        # These defaults include all rows no matter the existing flag value.
        expected_flags: IndexerPushdataRegistrationFlag=IndexerPushdataRegistrationFlag.NONE,
        mask: IndexerPushdataRegistrationFlag=IndexerPushdataRegistrationFlag.NONE,
        db: Optional[sqlite3.Connection]=None) -> None:
    assert db is not None and isinstance(db, sqlite3.Connection)

    account_id = create_account_write(external_account_id, db)

    sql = """
    DELETE FROM indexer_filtering_registrations_pushdata
    WHERE account_id=? AND pushdata_hash=? AND flags&?=?
    """
    update_rows: list[tuple[int, bytes, int, int]] = []
    for pushdata_value in pushdata_hashes:
        update_rows.append((account_id, pushdata_value, mask, expected_flags))
    db.executemany(sql, update_rows)


def create_outbound_data_write(creation_row: OutboundDataRow,
        db: Optional[sqlite3.Connection]=None) -> int:
    assert db is not None and isinstance(db, sqlite3.Connection)
    sql = "INSERT INTO outbound_data (outbound_data_id, outbound_data, outbound_data_flags, " \
        "date_created, date_last_tried) VALUES (?, ?, ?, ?, ?) RETURNING outbound_data_id"
    result_row = db.execute(sql, creation_row).fetchone()
    if result_row is None:
        raise DatabaseInsertConflict()
    return cast(int, result_row[0])


@replace_db_context_with_connection
def read_pending_outbound_datas(db: sqlite3.Connection, flags: OutboundDataFlag,
        mask: OutboundDataFlag) -> list[OutboundDataRow]:
    sql = "SELECT outbound_data_id, outbound_data, outbound_data_flags, date_created, " \
        "date_last_tried FROM outbound_data WHERE (outbound_data_flags&?)=? " \
        "ORDER BY date_last_tried ASC"
    sql_values = (mask, flags)
    return [ OutboundDataRow(row[0], row[1], OutboundDataFlag(row[2]), row[3], row[4])
        for row in db.execute(sql, sql_values) ]


def update_outbound_data_last_tried_write(entries: list[tuple[OutboundDataFlag, int, int]],
        db: Optional[sqlite3.Connection]=None) -> None:
    assert db is not None and isinstance(db, sqlite3.Connection)
    sql = "UPDATE outbound_data SET outbound_data_flags=?, date_last_tried=? " \
        "WHERE outbound_data_id=?"
    cursor = db.executemany(sql, entries)
    assert cursor.rowcount == len(entries)

