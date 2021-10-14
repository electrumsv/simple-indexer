import io
import logging
import queue
import threading
import time
import typing

import bitcoinx
import zmq
from bitcoinx import hex_str_to_hash
from electrumsv_node import electrumsv_node

from .constants import ZMQ_NODE_PORT, ZMQ_TOPIC_HASH_BLOCK, ZMQ_TOPIC_HASH_TX
from .parse_pushdata import get_pushdata_from_script
from .utils import wait_for_initial_node_startup
from .sqlite_db import SQLiteDatabase

if typing.TYPE_CHECKING:
    from .server import ApplicationState


class BlockchainStateMonitor(threading.Thread):

    def __init__(self, app_state: 'ApplicationState', ws_queue: queue.Queue[str]):
        threading.Thread.__init__(self, daemon=True)
        self.logger = logging.getLogger("blockchain-state-monitor")
        self.app_state = app_state
        self.ws_queue = ws_queue
        self.sqlite_db = SQLiteDatabase()

        wait_for_initial_node_startup(self.logger)
        # NOTE: These are NOT daemon threads so must take care that all of them shutdown gracefully
        # and do not get "stuck". Otherwise the main thread will stay running.
        # The alternative is uncontrolled killing of the threads (because they'd be daemon threads)
        # but I find that unacceptable for this use-case.
        zmq_block_hash_thread = threading.Thread(target=self.zmq_subscribe_to_block_hash)
        zmq_tx_hash_thread = threading.Thread(target=self.zmq_subscribe_to_tx_hash)

        self.threads = (
            zmq_block_hash_thread,
            zmq_tx_hash_thread,
        )

    def run(self):
        try:
            for t in self.threads:
                t.start()
            while self.app_state.app.is_alive:
                time.sleep(1)
        except Exception:
            self.logger.exception("unexpected exception in BlockchainStateMonitor")
        finally:
            self.logger.info("Closing BlockchainStateMonitor thread")

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

    def parse_block(self, block_hash: bytes, rawblock_stream: io.BytesIO):
        raw_header = rawblock_stream.read(80)  # Todo add a block headers table and insert this there
        tx_count = bitcoinx.read_varint(rawblock_stream.read)
        txs = []
        for i in range(tx_count):
            tx = bitcoinx.Tx.read(rawblock_stream.read)
            txs.append(tx)

        self.insert_tx_rows(block_hash, txs)
        self.insert_txo_rows(txs)
        self.insert_input_rows(txs)
        self.insert_pushdata_rows(txs)


    def on_block(self, block_hash: str) -> None:
        self.logger.debug(f"Got blockhash: {block_hash}")
        rawblock_hex = electrumsv_node.call_any('getblock', block_hash, 0).json()['result']
        rawblock = bytes.fromhex(rawblock_hex)
        rawblock_stream = io.BytesIO(rawblock)
        self.parse_block(hex_str_to_hash(block_hash), rawblock_stream)

    def on_mempool_tx(self, tx_hash):
        self.logger.debug(f"Got tx_hash: {tx_hash}")

    # Thread -> push to queue
    # zmq.NOBLOCK mode is used so that the loop has the opportunity to check for 'app.is_alive'
    # To me this is cleaner than a daemon=True thread where the thread dies in an uncontrolled way
    def zmq_subscribe_to_block_hash(self):
        logger = logging.getLogger("zmq-hashblock-thread")
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
                        logger.debug(f"got {block_hash} from 'hashblock' sub")
                        self.on_block(block_hash)
                except zmq.error.Again:
                    continue
                except Exception:
                    logger.exception("unexpected exception in 'zmq_subscribe_to_block_hash' thread")
        finally:
            logger.info("Closing zmq_subscribe_to_block_hash thread")
            work_receiver.close()
            context.term()

    # Thread -> push to queue
    # zmq.NOBLOCK mode is used so that the loop has the opportunity to check for 'app.is_alive'
    # To me this is cleaner than a daemon=True thread where the thread dies in an uncontrolled way
    def zmq_subscribe_to_tx_hash(self):
        logger = logging.getLogger("zmq-hashtx-thread")
        context: zmq.Context = zmq.Context()
        work_receiver: zmq.Socket = context.socket(zmq.SUB)
        try:
            work_receiver.connect(f"tcp://127.0.0.1:{ZMQ_NODE_PORT}")
            work_receiver.subscribe(ZMQ_TOPIC_HASH_TX)
            while self.app_state.app.is_alive:
                try:
                    if work_receiver.poll(1000, zmq.POLLIN):
                        msg = work_receiver.recv(zmq.NOBLOCK)
                        # logger.debug(f"got message {msg}")
                    else:
                        continue

                    if msg == ZMQ_TOPIC_HASH_TX:
                        continue
                    if len(msg) == 32:
                        tx_hash = msg.hex()
                        logger.debug(f"got {tx_hash} from 'hashtx' sub")
                        self.on_mempool_tx(tx_hash)
                except zmq.error.Again:
                    continue
                except Exception:
                    logger.exception("unexpected exception in 'zmq_subscribe_to_tx_hash' thread")
        finally:
            logger.info("Closing zmq_subscribe_to_block_hash thread")
            work_receiver.close()
            context.term()
