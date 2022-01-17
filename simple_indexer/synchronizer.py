import io
import logging
import queue
import threading
import time
from typing import Optional, TYPE_CHECKING

import bitcoinx
from bitcoinx import hex_str_to_hash, hash_to_hex_str
from electrumsv_node import electrumsv_node
import requests
import zmq

from .constants import ZMQ_NODE_PORT, ZMQ_TOPIC_HASH_BLOCK, ZMQ_TOPIC_HASH_TX, \
    NULL_HASH, GENESIS_HASH
from .parse_pushdata import get_pushdata_from_script
from .types import OutpointType, output_spend_struct, OutputSpendRow
from .utils import wait_for_initial_node_startup

if TYPE_CHECKING:
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

    def __init__(self, app_state: 'ApplicationState', ws_queue: queue.Queue[bytes]):
        threading.Thread.__init__(self, daemon=True)
        self.logger = logging.getLogger("blockchain-state-monitor")
        self.app_state = app_state
        self.ws_queue = ws_queue
        self.sqlite_db = self.app_state.sqlite_db
        self.is_ibd = False  # Set to True when initial block download is complete

        self._unspent_output_registrations: set[OutpointType] = set()

        wait_for_initial_node_startup(self.logger)
        # NOTE: These are NOT daemon threads so must take care that all of them shutdown gracefully
        # and do not get "stuck". Otherwise the main thread will stay running.
        # The alternative is uncontrolled killing of the threads (because they'd be daemon threads)
        # but I find that unacceptable for this use-case.
        self._initial_sync_complete = threading.Event()

        maintain_chain_tip_thread = threading.Thread(target=self.maintain_chain_tip_thread)
        poll_node_thread = threading.Thread(target=self.poll_node_tip_thread)
        process_mempool_txs_thread = threading.Thread(target=self.process_new_txs_thread)
        self.threads = (
            maintain_chain_tip_thread,
            poll_node_thread,
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

        self.insert_tx_rows(block_hash, txs)
        for tx in txs:
            tx_hash = tx.hash()
            if tx_hash in unprocessed_txs_hashes:
                self.insert_txo_rows([tx])
                self.insert_input_rows([tx])
                self.insert_pushdata_rows([tx])

            # Dispatch any spent output notifications.
            for in_idx, tx_input in enumerate(tx.inputs):
                outpoint = tx_input.prev_hash, tx_input.prev_idx
                if outpoint in self._unspent_output_registrations:
                    self._broadcast_spent_output_event(tx_input.prev_hash, tx_input.prev_idx,
                            tx_hash, in_idx, None)

        # Todo - atomically invalidate mempool +
        #  update local indexer tip (in both headers.mmap + app_state.local_tip attribute)
        all_mined_tx_hashes = processed_tx_hashes | unprocessed_txs_hashes
        # self.logger.debug(f"Invalidating tx_hashes: {[hash_to_hex_str(x) for x in all_mined_tx_hashes]}")
        self.sqlite_db.invalidate_mempool(all_mined_tx_hashes)

    def on_block(self, new_tip: bitcoinx.Header) -> None:
        block_hash_hex = hash_to_hex_str(new_tip.hash)
        # self.logger.debug(f"Got blockhash: {block_hash_hex}")
        rawblock_hex = electrumsv_node.call_any('getblock', block_hash_hex, 0).json()['result']
        rawblock = bytes.fromhex(rawblock_hex)
        rawblock_stream = io.BytesIO(rawblock)
        self.parse_block(new_tip.hash, rawblock_stream)
        self.sqlite_db.insert_block_row(new_tip.hash, new_tip.height, rawblock)

    def on_tx(self, tx_id: str):
        rawtx = electrumsv_node.call_any('getrawtransaction', tx_id, 1).json()['result']
        tx = bitcoinx.Tx.from_hex(rawtx['hex'])
        is_confirmed = rawtx.get('blockhash', False)
        is_mempool = not is_confirmed
        if is_mempool:
            # There is no batch-wise processing because we don't care about performance
            self.logger.debug(f"Got mempool tx_hash: {tx_id}")
            self.insert_mempool_tx_rows([tx])
            self.insert_txo_rows([tx])
            self.insert_input_rows([tx])
            self.insert_pushdata_rows([tx])

            tx_hash = hex_str_to_hash(tx_id)
            for in_idx, tx_input in enumerate(tx.inputs):
                outpoint = tx_input.prev_hash, tx_input.prev_idx
                if outpoint in self._unspent_output_registrations:
                    self._broadcast_spent_output_event(tx_input.prev_hash, tx_input.prev_idx,
                            tx_hash, in_idx, None)
        else:
            pass

    def _broadcast_spent_output_event(self, out_tx_hash: bytes, out_idx: int,
            in_tx_hash: bytes, in_idx: int, block_hash: Optional[bytes]) -> None:
        message_bytes = output_spend_struct.pack(out_tx_hash, out_idx, in_tx_hash, in_idx,
            block_hash)
        # We do not provide any kind of message envelope at this time as this is the only
        # kind of message we send.
        self.ws_queue.put(message_bytes, block=False)

    def connect_header(self, height: int, raw_header: bytes, headers_store: str):
        try:
            if headers_store == 'node':
                self.app_state.node_headers.connect(raw_header)
                self.app_state.node_headers.flush()
                # self.logger.debug(f"Connected header (node store) height: {height}; "
                #                   f"hash: {hash_to_hex_str(double_sha256(raw_header))}")
            else:
                self.app_state.local_headers.connect(raw_header)
                self.app_state.local_headers.flush()
                # self.logger.debug(f"Connected header (local store) tip height: {height}; "
                #                   f"hash: {hash_to_hex_str(double_sha256(raw_header))}")

        except bitcoinx.MissingHeader as e:
            if str(e).find(GENESIS_HASH) != -1 or str(e).find(NULL_HASH) != -1:
                if headers_store == 'node':
                    self.app_state.node_headers.set_one(height, raw_header)
                    self.app_state.node_headers.flush()
                    # self.logger.debug("Got genesis block or null hash")
                else:
                    self.app_state.local_headers.set_one(height, raw_header)
                    self.app_state.local_headers.flush()
                    # self.logger.debug("Got genesis block or null hash")
            else:
                # self.logger.exception(e)
                raise

    def sync_node_block_headers(self, to_height: int, from_height: int=0,
            headers_store: str='node'):

        for height in range(from_height, to_height + 1):
            block_hash = electrumsv_node.call_any('getblockhash', height).json()['result']
            block_header: str = electrumsv_node.call_any('getblockheader', block_hash, False).json()['result']
            self.connect_header(height, bytes.fromhex(block_header), headers_store=headers_store)

    def backfill_headers(self, to_height: int):
        # Just start at genesis and resync all headers (we do not care about performance)
        self.sync_node_block_headers(to_height, from_height=0)

    def find_common_parent(self, reorg_node_tip: bitcoinx.Header,
            orphaned_tip: bitcoinx.Header) -> tuple[bitcoinx.Chain, int]:
        chains: list[bitcoinx.Chain] = self.app_state.node_headers.chains()

        # Get orphan an reorg chains
        orphaned_chain = None
        reorg_chain = None
        for chain in chains:
            if chain.tip.hash == reorg_node_tip.hash:
                reorg_chain = chain
            if chain.tip.hash == orphaned_tip.hash:
                orphaned_chain = chain

        if reorg_chain is not None and orphaned_chain is not None:
            common_chain_and_height = reorg_chain.common_chain_and_height(orphaned_chain)
            return common_chain_and_height
        else:
            # Should never happen
            raise ValueError("No common parent block header could be found")

    def on_reorg(self, orphaned_tip: bitcoinx.Header, new_best_tip: int):
        # Track down any missing node headers and add them to the 'node_headers' store
        count_chains_before = len(self.app_state.node_headers.chains())
        self.backfill_headers(new_best_tip)
        count_chains_after_backfill = len(self.app_state.node_headers.chains())
        if count_chains_after_backfill > count_chains_before:
            reorg_new_tip = self.app_state.node_headers.longest_chain().tip
            chain, common_parent_height = self.find_common_parent(reorg_new_tip, orphaned_tip)

            depth = reorg_new_tip.height - common_parent_height - 1
            self.logger.debug(f"Reorg detected of depth: {depth}. Syncing blocks from parent height: "
                              f"{common_parent_height} to {reorg_new_tip.height}")

            for height in range(common_parent_height, new_best_tip + 1):
                block_hash = electrumsv_node.call_any('getblockhash', height).json()['result']
                header: bitcoinx.Header = self.app_state.node_headers.lookup(hex_str_to_hash(block_hash))[0]
                self.on_block(header)
                self.connect_header(height, header.raw, headers_store='local')

    def sync_blocks(self, from_height=0, to_height=0):
        for height in range(from_height, to_height + 1):
            block_hash = electrumsv_node.call_any('getblockhash', height).json()['result']
            header: bitcoinx.Header = \
                self.app_state.node_headers.lookup(hex_str_to_hash(block_hash))[0]
            self.on_block(header)
            self.connect_header(height, header.raw, headers_store='local')

    def on_new_tip(self, block_hash_new_tip: str):
        """This should only be called after initial block download"""
        stored_node_tip = self.app_state.node_headers.longest_chain().tip
        new_best_tip: dict = electrumsv_node.call_any('getblockheader', block_hash_new_tip, True).json()['result']
        self.logger.info(f"New tip received height: {new_best_tip['height']}. "
                          f"Stored node height: {stored_node_tip.height}")
        try:
            self.sync_node_block_headers(to_height=new_best_tip['height'],
                from_height=new_best_tip['height'])
            self.sync_blocks(from_height=new_best_tip['height'], to_height=new_best_tip['height'])
        except bitcoinx.MissingHeader:
            self.on_reorg(stored_node_tip, new_best_tip['height'])
        finally:
            new_tip = self.app_state.node_headers.longest_chain().tip
            self.logger.info(f"New best tip height: {new_tip}, "
                             f"hash: {hash_to_hex_str(new_tip.hash)}")

    # Thread -> push to queue
    # zmq.NOBLOCK mode is used so that the loop has the opportunity to check for 'app.is_alive'
    # To me this is cleaner than a daemon=True thread where the thread dies in an uncontrolled way
    def maintain_chain_tip_thread(self):
        logger = logging.getLogger("maintain-chain-tip-thread")

        result = electrumsv_node.call_any('getblockchaininfo').json()['result']
        node_tip_height = result['headers']

        self.sync_node_block_headers(node_tip_height, from_height=0)

        while node_tip_height > self.app_state.local_headers.longest_chain().tip.height:
            local_tip_height = self.app_state.local_headers.longest_chain().tip.height
            new_header: bitcoinx.Header = self.app_state.node_headers.header_at_height(
                self.app_state.node_headers.longest_chain(), local_tip_height+1)
            self.on_block(new_header)
            self.connect_header(local_tip_height+1, new_header.raw, headers_store='local')

        self._initial_sync_complete.set()

        new_tip = self.app_state.node_headers.longest_chain().tip
        self.logger.info(f"New best tip height: {new_tip.height}, "
            f"hash: {hash_to_hex_str(new_tip.hash)}")
        self.logger.debug("Initial block download complete. Waiting for the next block...")
        self.is_ibd = True
        self.logger.debug("Requesting mempool...")
        mempool_tx_hashes = electrumsv_node.call_any('getrawmempool').json()['result']
        for tx_hash in mempool_tx_hashes:
            self.on_tx(tx_hash)

        self.logger.debug("Listening to node ZMQ...")
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

    def poll_node_tip_thread(self):
        """Continually poll the node for chain tip every 5 seconds. This is because ZMQ
        notifications are not received for RegTest blocks with old timestamps so if the node
        only has old timestamp blocks loaded, we can only find out via the RPC API."""
        logger = logging.getLogger("poll-node-tip-thread")
        self._initial_sync_complete.wait()
        logger.info("Starting thread to poll for node tip")
        while self.app_state.app.is_alive:
            try:
                result = electrumsv_node.call_any('getblockchaininfo').json()['result']
                node_tip_height = result['headers']
                while node_tip_height > self.app_state.local_headers.longest_chain().tip.height:
                    node_tip_height = electrumsv_node.call_any('getblockchaininfo') \
                                                     .json()['result']['headers']
                    local_height = self.app_state.local_headers.longest_chain().tip.height
                    next_block_hash_in_sequence: str = \
                        electrumsv_node.call_any('getblockhash', local_height + 1).json()['result']
                    self.on_new_tip(next_block_hash_in_sequence)
            except requests.exceptions.HTTPError:
                logger.info("Error polling node. Retrying in 5 seconds")
            finally:
                time.sleep(5)


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

    def register_output_spend_notifications(self, outpoints: list[OutpointType]) \
            -> list[OutputSpendRow]:
        # We register all the provided outpoints for notifications.
        # - We do this before we query the results to ensure there is overlap on getting the
        #   current state and broadcasting changes, so that the caller does not miss out on
        #   events that might otherwise be between a get state followed by a register for
        #   notifications.
        # - Ideally registered outpoints would be pruned as blocks get enough confirmations and
        #   reorgs are no longer possible. But this is the simple indexer, it is enough to note
        #   this flaw and move on.
        # - The sole consumer of this API is the reference server, and it can track what where
        #   to pass notifications for these events.
        for outpoint in outpoints:
            self._unspent_output_registrations.add(outpoint)

        # We need to find out if the outpoints are spent, and where. We can return the results
        # immediately. Otherwise we should send notifications on any open web socket.
        return self.sqlite_db.get_spent_outpoints(outpoints)

