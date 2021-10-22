"""
Procedure for generating reorging chain is as follows:
- Run the simple indexer and fill the SQLiteDB with blockchain_115_36_3677f4
- Query the SQLiteDB to extract all non-coinbase txs above height 110 and save them to file (there are 22 of them)
- Reset the node to height zero
- Take blockchain_115_3677f4 and submit only the first 110 blocks
(i.e exclude the last 5 with the ElectrumSV test transactions in them)
- Mine 5 random blocks from 110 -> 115
- Submit all 22 transactions to mempool and mine block 116
- All txs are now "reorged" to height 116
- Save this set of blocks to blockchain_116... for use in reproducible reorg testing
"""
import os
from pathlib import Path

import bitcoinx
from electrumsv_node import electrumsv_node
from electrumsv_sdk import commands

from contrib.scripts.import_blocks import import_blocks
from simple_indexer.sqlite_db import SQLiteDatabase

MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
BLOCKCHAIN_PATH = MODULE_DIR.parent / 'blockchains' / 'blockchain_115_3677f4'

os.environ['SIMPLE_INDEX_RESET'] = '0'


def extract_relevant_non_coinbase_txs():
    MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
    sqlitedb = SQLiteDatabase(MODULE_DIR.parent.parent / 'simple_index.db')
    result = sqlitedb.execute("""
        SELECT rawtx FROM confirmed_transactions AS ct
            JOIN blocks ON ct.block_hash = blocks.block_hash
            WHERE block_height > 110 ORDER BY block_height ASC;""")

    with open('tx_dump.hex', 'w') as f:
        for row in result:
            rawtx = row[0]
            # print(bitcoinx.Tx.from_bytes(rawtx))
            f.write(rawtx.hex() + "\n")


def reset_node():
    commands.stop(component_type='node')
    commands.reset(component_type='node')
    commands.start(component_type='node')


def submit_transactions():
    non_coinbase_txs = {}
    with open('tx_dump.hex', 'r') as f:
        lines = f.readlines()
        for line in lines:
            rawtx = line.strip()
            tx = bitcoinx.Tx.from_hex(rawtx)
            if not tx.is_coinbase():
                non_coinbase_txs[tx.hash()] = tx

        print(f"Non-coinbase count: {len(non_coinbase_txs)}")

    while len(non_coinbase_txs) != 0:
        successes = []
        for tx_hash, tx in non_coinbase_txs.items():
            try:
                electrumsv_node.call_any('sendrawtransaction', tx.to_hex())
                successes.append(tx_hash)
            except Exception as e:
                # It's hard to know the correct order of submitting the txs so use trial and error
                print(e)

        for tx_hash in successes:
            del non_coinbase_txs[tx_hash]


# NOTE: You must first run simple indexer with blockchain_115_36_3677f4 then run this script
extract_relevant_non_coinbase_txs()  # From blockchain_115_36_3677f4
reset_node()
import_blocks(BLOCKCHAIN_PATH, to_height=110)  # From blockchain_115_36_3677f4 to height 110 of 115
electrumsv_node.call_any('generate', 5)  # Random 5 blocks
submit_transactions()  # Same txs from blockchain_115_36_3677f4 will now be mined at height 116
electrumsv_node.call_any('generate', 1)
