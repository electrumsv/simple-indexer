import io
import logging
import queue
import threading
import time
import typing

import bitcoinx
import zmq
from bitcoinx import hex_str_to_hash, hash_to_hex_str
from electrumsv_node import electrumsv_node

from .constants import ZMQ_NODE_PORT, ZMQ_TOPIC_HASH_BLOCK, ZMQ_TOPIC_HASH_TX, \
    NULL_HASH, GENESIS_HASH
from .parse_pushdata import get_pushdata_from_script
from .utils import wait_for_initial_node_startup
from .sqlite_db import SQLiteDatabase

if typing.TYPE_CHECKING:
    from .server import ApplicationState


class Synchronizer(threading.Thread):
    """
    Mempool transactions will be continually parsed and indexed right from startup.
    There will be no concept of 'initial block download' as we do not care about scaling for this
    indexer. The mempool parsing thread is always running right from startup.

    Mempool txs are invalidated from the "cache" (which is actually a db table like any other)
    when a block is mined containing those transactions.

    If there is a reorg event, transactions are put BACK TO the mempool tx table on "rewind"
    and then re-invalidated on "rolling forward" to get back to a consistent state. While this is
    happening there will be a "stop-the-world" style freeze on API queries until the state is
    once again consistent for servicing requests via the External API. Seeing as though this will
    only be dealing with RegTest volumes, the freeze time window should not be noticeable.

    Txos, inputs, pushdatas are all flushed to the respective tables for a mempool tx so we sort the
    confirmed txs in a block into two categories:
        - Processed
        - Unprocessed
    If the tx has already been processed (characterised by its presence in the mempool) then we only
    need to invalidate it from the mempool table and add it to the confirmed tx table.

    If the tx has NOT already been processed (not present in the mempool) then we must parse it
    in full and insert the relevant rows to the txos, inputs and pushdata tables in addition to
    the row for the confirmed tx table. In this RegTest case this should only happen if there
    is activity on the node while the indexer is offline (and so misses those mempool events).

    Of course the coinbase in each block will never be present in the mempool so there will always
    be at least 1 tx in the "Unprocessed" category for each block.
    """

    def __init__(self, app_state: 'ApplicationState', ws_queue: queue.Queue[str]):
        threading.Thread.__init__(self, daemon=True)
        self.logger = logging.getLogger("blockchain-state-monitor")
        self.app_state = app_state
        self.ws_queue = ws_queue
        self.sqlite_db = SQLiteDatabase()
        self.is_ibd = False  # Set to True when initial block download is complete

        wait_for_initial_node_startup(self.logger)
        # NOTE: These are NOT daemon threads so must take care that all of them shutdown gracefully
        # and do not get "stuck". Otherwise the main thread will stay running.
        # The alternative is uncontrolled killing of the threads (because they'd be daemon threads)
        # but I find that unacceptable for this use-case.
        maintain_chain_tip_thread = threading.Thread(target=self.maintain_chain_tip_thread)
        process_mempool_txs_thread = threading.Thread(target=self.process_new_txs_thread)

        self.threads = (
            maintain_chain_tip_thread,
            process_mempool_txs_thread,
        )

    def run(self):
        try:
            for t in self.threads:
                t.start()
            while self.app_state.app.is_alive:
                time.sleep(1)
        except Exception:
            self.logger.exception("unexpected exception in Synchronizer")
        finally:
            self.logger.info("Closing Synchronizer thread")

    def insert_tx_rows(self, block_hash: bytes, txs: list[bitcoinx.Tx]):
        tx_rows = []
        for idx, tx in enumerate(txs):
            tx_hash = tx.hash()
            block_hash = block_hash
            tx_position = idx
            rawtx = tx.to_bytes()
            tx_row = (tx_hash, block_hash, tx_position, rawtx)
            tx_rows.append(tx_row)

        self.sqlite_db.insert_tx_rows(tx_rows)

    def insert_mempool_tx_rows(self, txs: list[bitcoinx.Tx]):
        tx_rows = []
        for idx, tx in enumerate(txs):
            mp_tx_hash = tx.hash()
            mp_tx_timestamp = time.time()
            rawtx = tx.to_bytes()
            tx_row = (mp_tx_hash, mp_tx_timestamp, rawtx)
            tx_rows.append(tx_row)

        self.sqlite_db.insert_mempool_tx_rows(tx_rows)

    def insert_txo_rows(self, txs: list[bitcoinx.Tx]):
        output_rows = []
        for tx in txs:
            for idx, output in enumerate(tx.outputs):
                out_tx_hash = tx.hash()
                out_idx = idx
                out_value = output.value
                out_scriptpubkey = output.script_pubkey.to_bytes()
                output_row = (out_tx_hash, out_idx, out_value, out_scriptpubkey)
                output_rows.append(output_row)

        self.sqlite_db.insert_txo_rows(output_rows)

    def insert_input_rows(self, txs: list[bitcoinx.Tx]):
        input_rows = []
        for tx in txs:
            for idx, input in enumerate(tx.inputs):
                out_tx_hash = input.prev_hash
                out_idx = input.prev_idx
                in_tx_hash = tx.hash()
                in_idx = idx
                in_scriptsig = input.script_sig.to_bytes()
                input_row = (out_tx_hash, out_idx, in_tx_hash, in_idx, in_scriptsig)
                input_rows.append(input_row)

        self.sqlite_db.insert_input_rows(input_rows)

    def insert_pushdata_rows(self, txs: list[bitcoinx.Tx]):
        pushdata_rows = []
        for tx in txs:
            tx_hash = tx.hash()
            for in_idx, input in enumerate(tx.inputs):
                in_scriptsig = input.script_sig.to_bytes()
                input_pushdatas = get_pushdata_from_script(in_scriptsig)
                if input_pushdatas:
                    for pushdata_hash in input_pushdatas:
                        ref_type = 1  # An input
                        pushdata_row = (pushdata_hash, tx_hash, in_idx, ref_type)
                        pushdata_rows.append(pushdata_row)

            for out_idx, output in enumerate(tx.outputs):
                out_scriptpubkey = output.script_pubkey.to_bytes()
                output_pushdatas = get_pushdata_from_script(out_scriptpubkey)
                if output_pushdatas:
                    for pushdata_hash in output_pushdatas:
                        ref_type = 0  # An output
                        pushdata_row = (pushdata_hash, tx_hash, out_idx, ref_type)
                        pushdata_rows.append(pushdata_row)

        self.sqlite_db.insert_pushdata_rows(pushdata_rows)

    def get_processed_vs_unprocessed_txs(self, txs: list[bitcoinx.Tx]) \
            -> tuple[set[bytes], set[bytes]]:
        """Feed the tx hashes in the block to this SELECT query to see which have already been
        processed via the mempool"""
        tx_hashes = set(tx.hash() for tx in txs)
        processed_tx_hashes = self.sqlite_db.get_matching_mempool_txids(tx_hashes)
        unprocessed_tx_hashes = tx_hashes - processed_tx_hashes
        return processed_tx_hashes, unprocessed_tx_hashes

    def parse_block(self, block_hash: bytes, rawblock_stream: io.BytesIO):
        # Todo add a block headers table and insert this there
        # Todo - detect reorg -> if reorg -> handle_reorg()
        raw_header = rawblock_stream.read(80)
        tx_count = bitcoinx.read_varint(rawblock_stream.read)
        txs = []
        for i in range(tx_count):
            tx = bitcoinx.Tx.read(rawblock_stream.read)
            txs.append(tx)

        processed_tx_hashes, unprocessed_txs_hashes = self.get_processed_vs_unprocessed_txs(txs)

        for tx in txs:
            if tx.hash() in processed_tx_hashes:
                self.insert_tx_rows(block_hash, txs)

            if tx.hash() in unprocessed_txs_hashes:
                self.insert_tx_rows(block_hash, txs)
                self.insert_txo_rows(txs)
                self.insert_input_rows(txs)
                self.insert_pushdata_rows(txs)

        # Todo - atomically invalidate mempool +
        #  update local indexer tip (in both headers.mmap + app_state.local_tip attribute)
        all_mined_tx_hashes = processed_tx_hashes | unprocessed_txs_hashes
        self.logger.debug(f"Invalidating tx_hashes: {[hash_to_hex_str(x) for x in all_mined_tx_hashes]}")
        self.sqlite_db.invalidate_mempool(all_mined_tx_hashes)

    def on_block(self, new_tip: bitcoinx.Header) -> None:
        block_hash_hex = hash_to_hex_str(new_tip.hash)
        self.logger.debug(f"Got blockhash: {block_hash_hex}")
        rawblock_hex = electrumsv_node.call_any('getblock', block_hash_hex, 0).json()['result']
        rawblock = bytes.fromhex(rawblock_hex)
        rawblock_stream = io.BytesIO(rawblock)
        self.parse_block(new_tip.hash, rawblock_stream)

    def on_tx(self, tx_hash: str):
        rawtx = electrumsv_node.call_any('getrawtransaction', tx_hash, 1).json()['result']
        tx = bitcoinx.Tx.from_hex(rawtx['hex'])
        is_confirmed = rawtx.get('blockhash', False)
        is_mempool = not is_confirmed
        if is_mempool:
            # There is no batch-wise processing because we don't care about performance
            self.logger.debug(f"Got mempool tx_hash: {tx_hash}")
            self.insert_mempool_tx_rows([tx])
            self.insert_txo_rows([tx])
            self.insert_input_rows([tx])
            self.insert_pushdata_rows([tx])
        else:
            pass

    def connect_header(self, height: int, raw_header: bytes, headers_store: str):
        try:
            if headers_store == 'node':
                self.app_state.node_headers.connect(raw_header)
                self.app_state.node_headers.flush()
                node_tip: bitcoinx.Header = self.app_state.node_headers.longest_chain().tip
                self.logger.debug(f"Connected header (node store) tip height: {node_tip.height}; "
                                  f"hash: {hash_to_hex_str(node_tip.hash)}")
            else:
                self.app_state.local_headers.connect(raw_header)
                self.app_state.local_headers.flush()
                local_tip: bitcoinx.Header = self.app_state.local_headers.longest_chain().tip
                self.logger.debug(f"Connected header (local store) tip height: {local_tip.height}; "
                                  f"hash: {hash_to_hex_str(local_tip.hash)}")

        except bitcoinx.MissingHeader as e:
            if str(e).find(GENESIS_HASH) != -1 or str(e).find(NULL_HASH) != -1:
                if headers_store == 'node':
                    self.app_state.node_headers.set_one(height, raw_header)
                    self.app_state.node_headers.flush()
                    self.logger.debug("Got genesis block or null hash")
                else:
                    self.app_state.local_headers.set_one(height, raw_header)
                    self.app_state.local_headers.flush()
                    self.logger.debug("Got genesis block or null hash")
            else:
                self.logger.exception(e)
                raise

    def sync_node_block_headers(self, node_tip_height: int):
        stored_node_tip = self.app_state.node_headers.longest_chain().tip.height
        for height in range(stored_node_tip, node_tip_height + 1):
            block_hash = electrumsv_node.call_any('getblockhash', height).json()['result']
            block_header: str = electrumsv_node.call_any('getblockheader', block_hash, False).json()['result']
            self.connect_header(height, bytes.fromhex(block_header), headers_store='node')

    def on_new_tip(self, block_hash_new_tip: str):
        """This should only be called after initial block download"""
        stored_node_tip = self.app_state.node_headers.longest_chain().tip
        block_header_new_tip: str = electrumsv_node.call_any('getblockheader', block_hash_new_tip, False).json()['result']
        self.connect_header(stored_node_tip.height+1, bytes.fromhex(block_header_new_tip), headers_store='node')
        self.connect_header(stored_node_tip.height+1, bytes.fromhex(block_header_new_tip), headers_store='local')

        tip: bitcoinx.Header = self.app_state.node_headers.longest_chain().tip
        self.on_block(tip)

    # Thread -> push to queue
    # zmq.NOBLOCK mode is used so that the loop has the opportunity to check for 'app.is_alive'
    # To me this is cleaner than a daemon=True thread where the thread dies in an uncontrolled way
    def maintain_chain_tip_thread(self):
        logger = logging.getLogger("maintain-chain-tip-thread")

        result = electrumsv_node.call_any('getblockchaininfo').json()['result']
        node_tip_height = result['headers']
        self.sync_node_block_headers(node_tip_height)

        while node_tip_height > self.app_state.local_headers.longest_chain().tip.height:
            local_tip_height = self.app_state.local_headers.longest_chain().tip.height
            new_header: bitcoinx.Header = self.app_state.node_headers.header_at_height(self.app_state.node_headers.longest_chain(), local_tip_height+1)
            self.on_block(new_header)
            self.connect_header(local_tip_height+1, new_header.raw, headers_store='local')

        self.logger.debug(f"Initial block download complete. Waiting for the next block...")
        self.is_ibd = True
        self.logger.debug(f"Requesting mempool...")
        mempool_tx_hashes = electrumsv_node.call_any('getrawmempool').json()['result']
        for tx_hash in mempool_tx_hashes:
            self.on_tx(tx_hash)

        # Wait on zmq pub/sub socket for new tip
        context: zmq.Context = zmq.Context()
        work_receiver: zmq.Socket = context.socket(zmq.SUB)
        try:
            work_receiver.connect(f"tcp://127.0.0.1:{ZMQ_NODE_PORT}")
            work_receiver.subscribe(ZMQ_TOPIC_HASH_BLOCK)
            while self.app_state.app.is_alive:
                try:
                    if work_receiver.poll(1000, zmq.POLLIN):
                        msg = work_receiver.recv(zmq.NOBLOCK)
                    else:
                        continue

                    if msg == ZMQ_TOPIC_HASH_BLOCK:
                        continue
                    if len(msg) == 32:


                        block_hash = msg.hex()
                        logger.debug(f"Got {block_hash} from 'hashblock' sub")
                        self.on_new_tip(block_hash)
                except zmq.error.Again:
                    continue
                except Exception:
                    logger.exception("unexpected exception in 'maintain_chain_tip_thread' thread")
        finally:
            logger.info("Closing maintain_chain_tip_thread thread")
            work_receiver.close()
            context.term()

    # Thread -> push to queue
    # zmq.NOBLOCK mode is used so that the loop has the opportunity to check for 'app.is_alive'
    # To me this is cleaner than a daemon=True thread where the thread dies in an uncontrolled way
    def process_new_txs_thread(self):
        logger = logging.getLogger("process-new-txs-thread")
        context: zmq.Context = zmq.Context()
        work_receiver: zmq.Socket = context.socket(zmq.SUB)
        try:
            work_receiver.connect(f"tcp://127.0.0.1:{ZMQ_NODE_PORT}")
            work_receiver.subscribe(ZMQ_TOPIC_HASH_TX)
            while self.app_state.app.is_alive:
                try:
                    if work_receiver.poll(1000, zmq.POLLIN):
                        msg = work_receiver.recv(zmq.NOBLOCK)
                        # logger.debug(f"Got message {msg}")
                    else:
                        continue

                    if msg == ZMQ_TOPIC_HASH_TX:
                        continue
                    if len(msg) == 32:
                        tx_hash = msg.hex()
                        # logger.debug(f"Got {tx_hash} from 'hashtx' sub")
                        if self.is_ibd:
                            self.on_tx(tx_hash)
                except zmq.error.Again:
                    continue
                except Exception:
                    logger.exception("unexpected exception in 'zmq_mempool_tx_sub_thread' thread")
        finally:
            logger.info("Closing maintain_chain_tip_thread thread")
            work_receiver.close()
            context.term()
