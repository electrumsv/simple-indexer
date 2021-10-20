"""Much of this class and the connection pooling logic is inspired by and/or copied from the
ElectrumSV's wallet_database/sqlite_support.py and helps to avoid the overhead associated with
creating a new db connection"""

import logging
import os
import queue
import sqlite3
import threading
from pathlib import Path
from typing import Set, List, Optional

import bitcoinx


class LeakedSQLiteConnectionError(Exception):
    pass


def max_sql_variables() -> int:
    """Get the maximum number of arguments allowed in a query by the current
    sqlite3 implementation"""
    db = sqlite3.connect(':memory:')
    cur = db.cursor()
    cur.execute("CREATE TABLE t (test)")
    low, high = 0, 100000
    while (high - 1) > low:
        guess = (high + low) // 2
        query = 'INSERT INTO t VALUES ' + ','.join(['(?)' for _ in
                                                    range(guess)])
        args = [str(i) for i in range(guess)]
        try:
            cur.execute(query, args)
        except sqlite3.OperationalError as e:
            es = str(e)
            if "too many SQL variables" in es or "too many terms in compound SELECT" in es:
                high = guess
            else:
                raise
        else:
            low = guess
    cur.close()
    db.close()
    return low

# If the query deals with a list of values, then just batching using `SQLITE_MAX_VARS` should
# be enough. If it deals with expressions, then batch using the least of that and
# `SQLITE_EXPR_TREE_DEPTH`.
# - This shows how to estimate the maximum variables.
#   https://stackoverflow.com/a/36788489
# - This shows that even if you have higher maximum variables you get:
#   "Expression tree is too large (maximum depth 1000)"
#   https://github.com/electrumsv/electrumsv/issues/539
SQLITE_MAX_VARS = max_sql_variables()
SQLITE_EXPR_TREE_DEPTH = 1000


class SQLiteDatabase:
    """
    Due to connection pooling, all db operations (methods on this class) should be
    1) thread-safe
    2) low latency due to caching the connections prior to use
    """

    def __init__(self, storage_path: Path = Path('simple_index.db')):
        self.logger = logging.getLogger("sqlite-database")
        self.storage_path = storage_path
        self.conn = sqlite3.connect(self.storage_path)
        self._db_path = str(storage_path)
        self._connection_pool: queue.Queue[sqlite3.Connection] = queue.Queue()
        self._active_connections: Set[sqlite3.Connection] = set()
        self.mined_tx_hashes_table_lock = threading.RLock()

        if int(os.getenv('SIMPLE_INDEX_RESET', "1")):
            self.reset_tables()
        else:  # create if not exist
            self.create_tables()

    def get_path(self) -> str:
        return self._db_path

    def acquire_connection(self) -> sqlite3.Connection:
        try:
            conn = self._connection_pool.get_nowait()
        except queue.Empty:
            self.increase_connection_pool()
            conn = self._connection_pool.get_nowait()
        self._active_connections.add(conn)
        return conn

    def release_connection(self, connection: sqlite3.Connection) -> None:
        self._active_connections.remove(connection)
        self._connection_pool.put(connection)

    def increase_connection_pool(self) -> None:
        """adds 1 more connection to the pool"""
        connection = sqlite3.connect(self._db_path, check_same_thread=False)
        self._connection_pool.put(connection)

    def decrease_connection_pool(self) -> None:
        """release 1 more connection from the pool - raises empty queue error"""
        connection = self._connection_pool.get_nowait()
        connection.close()

    def close(self) -> None:
        # Force close all outstanding connections
        outstanding_connections = list(self._active_connections)
        for conn in outstanding_connections:
            self.release_connection(conn)

        while self._connection_pool.qsize() > 0:
            self.decrease_connection_pool()

        leak_count = len(outstanding_connections)
        if leak_count:
            raise LeakedSQLiteConnectionError(f"Leaked {leak_count} SQLite connections "
                "when closing DatabaseContext.")
        assert self.is_closed()

    def is_closed(self) -> bool:
        return self._connection_pool.qsize() == 0

    def execute(self, sql: str, params: Optional[tuple]=None) -> List:
        """Thread-safe"""
        connection = self.acquire_connection()
        try:
            if not params:
                cur: sqlite3.Cursor = connection.execute(sql)
            else:
                cur: sqlite3.Cursor = connection.execute(sql, params)
            connection.commit()
            return cur.fetchall()
        except sqlite3.IntegrityError as e:
            if str(e).find('UNIQUE constraint failed') != -1:
                self.logger.debug("caught constraint violation for attempting to insert the same "
                    "block twice")
        except Exception:
            connection.rollback()
            self.logger.exception(f"An unexpected exception occured for SQL: {sql}")
        finally:
            self.release_connection(connection)

    def create_tables(self):
        self.create_confirmed_tx_table()
        self.create_txos_table()
        self.create_mempool_tx_table()
        self.create_inputs_table()
        self.create_pushdata_table()

    def drop_tables(self):
        self.drop_confirmed_tx_table()
        self.drop_txos_table()
        self.drop_inputs_table()
        self.drop_pushdata_table()
        self.drop_mempool_tx_table()

    def reset_tables(self):
        self.drop_tables()
        self.create_tables()

    def create_confirmed_tx_table(self):
        sql = (
            """
            CREATE TABLE IF NOT EXISTS confirmed_transactions (
                tx_hash BINARY(32),
                block_hash BINARY(32),
                tx_position INTEGER,
                rawtx BLOB
            )"""
        )
        self.execute(sql)
        self.execute("CREATE INDEX IF NOT EXISTS tx_idx ON confirmed_transactions (tx_hash);")

    def drop_confirmed_tx_table(self):
        sql = (
            """DROP TABLE IF EXISTS confirmed_transactions"""
        )
        self.execute(sql)

    def create_mempool_tx_table(self):
        sql = (
            """
            CREATE TABLE IF NOT EXISTS mempool_transactions (
                mp_tx_hash BINARY(32),
                mp_tx_timestamp BINARY(32),
                rawtx BLOB
            )"""
        )
        self.execute(sql)
        self.execute("CREATE INDEX IF NOT EXISTS mp_tx_idx ON mempool_transactions (mp_tx_hash);")

    def drop_mempool_tx_table(self):
        sql = (
            """DROP TABLE IF EXISTS mempool_transactions"""
        )
        self.execute(sql)


    def create_txos_table(self):
        sql = (
            """
            CREATE TABLE IF NOT EXISTS txos (
                out_tx_hash BINARY(32),
                out_idx INTEGER,
                out_value INTEGER,
                out_scriptpubkey BLOB
            )"""
        )
        self.execute(sql)
        self.execute("CREATE INDEX IF NOT EXISTS txo_idx ON txos (out_tx_hash, out_idx);")

    def drop_txos_table(self):
        sql = (
            """DROP TABLE IF EXISTS txos"""
        )
        self.execute(sql)

    def create_inputs_table(self):
        sql = (
            """
            CREATE TABLE IF NOT EXISTS inputs (
                out_tx_hash BINARY(32),
                out_idx INTEGER,
                in_tx_hash BINARY(32),
                in_idx INTEGER,
                in_scriptsig BLOB
            )"""
        )
        self.execute(sql)
        self.execute("CREATE INDEX IF NOT EXISTS inputs_idx ON inputs (out_tx_hash, out_idx);")

    def drop_inputs_table(self):
        sql = (
            """DROP TABLE IF EXISTS inputs"""
        )
        self.execute(sql)

    def create_pushdata_table(self):
        sql = (
            """
            CREATE TABLE IF NOT EXISTS pushdata (
                pushdata_hash BINARY(32),
                tx_hash BINARY(32),
                idx INTEGER,
                ref_type SMALLINT
            )"""
        )
        self.execute(sql)
        self.execute("CREATE INDEX IF NOT EXISTS pushdata_idx ON pushdata (pushdata_hash, tx_hash, idx, ref_type);")

    def drop_pushdata_table(self):
        sql = (
            """DROP TABLE IF EXISTS pushdata"""
        )
        self.execute(sql)

    # ----- Database operations ----- #
    def insert_tx_rows(self, tx_rows: List[tuple]) -> None:
        # This is inefficient but it's not a priority
        for tx_row in tx_rows:
            sql = ("""INSERT INTO confirmed_transactions (
                        tx_hash,
                        block_hash,
                        tx_position,
                        rawtx
                   )
                   VALUES (?, ?, ?, ?)""")
            self.execute(sql, tx_row)

    def insert_mempool_tx_rows(self, tx_rows: List[tuple]) -> None:
        # This is inefficient but it's not a priority
        for tx_row in tx_rows:
            sql = ("""INSERT INTO mempool_transactions (
                        mp_tx_hash,
                        mp_tx_timestamp,
                        rawtx
                   )
                   VALUES (?, ?, ?)""")
            self.execute(sql, tx_row)

    def insert_txo_rows(self, txo_rows: List[tuple]) -> None:
        # This is inefficient but it's not a priority
        for output_row in txo_rows:
            sql = ("""INSERT INTO txos (
                        out_tx_hash,
                        out_idx,
                        out_value,
                        out_scriptpubkey
                   )
                   VALUES (?, ?, ?, ?)""")
            self.execute(sql, output_row)

    def insert_input_rows(self, input_rows: List[tuple]) -> None:
        # This is inefficient but it's not a priority
        for input_row in input_rows:
            sql = ("""INSERT INTO inputs (
                        out_tx_hash,
                        out_idx,
                        in_tx_hash,
                        in_idx,
                        in_scriptsig
                   )
                   VALUES (?, ?, ?, ?, ?)""")
            self.execute(sql, input_row)

    def insert_pushdata_rows(self, pushdata_rows: List[tuple]) -> None:
        # This is inefficient but it's not a priority
        for pushdata_row in pushdata_rows:
            sql = ("""INSERT INTO pushdata (
                        pushdata_hash,
                        tx_hash,
                        idx,
                        ref_type
                   )
                   VALUES (?, ?, ?, ?)""")
            self.execute(sql, pushdata_row)

    def create_temp_block_tx_hashes_table(self):
        """Used to join on mempool"""
        sql = (
            """
            CREATE TEMPORARY TABLE IF NOT EXISTS mined_tx_hashes (
                tx_hash BINARY(32)
            )"""
        )
        self.execute(sql)
        self.execute("CREATE INDEX IF NOT EXISTS temp_table_tx_idx ON mined_tx_hashes (tx_hash);")

    def drop_temp_block_hashes_table(self):
        sql = ("""DROP TABLE IF EXISTS mined_tx_hashes""")
        self.execute(sql)

    def get_matching_mempool_txids(self, tx_hashes: set[bytes]) -> set[bytes]:
        # Fill temporary table - one row at at time because we don't care about performance
        with self.mined_tx_hashes_table_lock:
            self.create_temp_block_tx_hashes_table()
            for tx_hash in tx_hashes:
                sql = ("""INSERT INTO mined_tx_hashes (tx_hash) VALUES (?)""")
                self.execute(sql, (tx_hash,))

            # Run SELECT query to find txs that have already been processed
            sql = ("""
                SELECT mp_tx_hash 
                FROM mempool_transactions 
                JOIN mined_tx_hashes ON tx_hash = mp_tx_hash;""")

            processed_tx_hashes = set()
            for row in self.execute(sql):
                tx_hash = row[0]
            processed_tx_hashes.add(tx_hash)
            self.drop_temp_block_hashes_table()
            return processed_tx_hashes

    def invalidate_mempool(self, tx_hashes: set[bytes]):
        for tx_hash in tx_hashes:
            sql = (f"""DELETE FROM mempool_transactions WHERE mp_tx_hash = ?""")
            self.execute(sql, (tx_hash,))
