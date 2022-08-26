import aiohttp
import asyncio
import concurrent.futures
from http import HTTPStatus
import json
import logging
import mmap
import os
from pathlib import Path
import queue
import sys
import threading
import time
from typing import Dict, Optional, TypeVar

from aiohttp import web
from bitcoinx import BitcoinRegtest, CheckPoint, hash_to_hex_str, Headers
from electrumsv_database.sqlite import DatabaseContext

from .constants import OutboundDataFlag, REFERENCE_SERVER_HOST, REFERENCE_SERVER_PORT, \
    REFERENCE_SERVER_SCHEME, SERVER_HOST, SERVER_PORT
from .handlers_ws import SimpleIndexerWebSocket, WSClient
from . import handlers
from . import sqlite_db
from .synchronizer import Synchronizer
from .types import OutboundDataRow, PushDataRow, TipFilterNotificationBatch, \
    TipFilterNotificationEntry


MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))

# Silence verbose logging
aiohttp_logger = logging.getLogger("aiohttp")
aiohttp_logger.setLevel(logging.WARNING)

logger = logging.getLogger("reference")


NODE_HEADERS_MMAP_FILEPATH = MODULE_DIR.parent / "localdata" / "node_headers.mmap"
LOCAL_HEADERS_MMAP_FILEPATH = MODULE_DIR.parent / "localdata" / "local_headers.mmap"
MMAP_SIZE = 100_000  # headers count - should be ample for RegTest
CHECKPOINT = CheckPoint(
    bytes.fromhex(
        "010000000000000000000000000000000000000000000000000000000000000000000000"
        "3ba3edfd7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa4b1e5e4adae5494d"
        "ffff7f2002000000"), height=0, prev_work=0
)

T2 = TypeVar("T2")


def asyncio_future_callback(future: asyncio.Task[None]) -> None:
    if future.cancelled():
        return
    future.result()

def future_callback(future: concurrent.futures.Future[None]) -> None:
    if future.cancelled():
        return
    future.result()


class ApplicationState(object):
    is_alive: bool = False

    def __init__(self, app: web.Application, loop: asyncio.AbstractEventLoop) -> None:
        self.logger = logging.getLogger('app_state')
        self.app = app
        self.loop = loop

        data_path = MODULE_DIR.parent / "localdata"
        data_path.mkdir(exist_ok=True)

        if int(os.getenv('SIMPLE_INDEX_RESET', "0")):
            self.reset_headers_stores()

        self.node_headers = Headers(BitcoinRegtest, NODE_HEADERS_MMAP_FILEPATH, CHECKPOINT)
        self.local_headers = Headers(BitcoinRegtest, LOCAL_HEADERS_MMAP_FILEPATH, CHECKPOINT)

        self.ws_clients: Dict[str, WSClient] = {}
        self.ws_clients_lock: threading.RLock = threading.RLock()

        self.ws_queue: queue.Queue[bytes] = queue.Queue()
        self.blockchain_state_monitor_thread: Optional[Synchronizer] = None

        scheme = os.getenv("REFERENCE_SERVER_SCHEME", REFERENCE_SERVER_SCHEME)
        host = os.getenv("REFERENCE_SERVER_HOST", REFERENCE_SERVER_HOST)
        port = os.getenv("REFERENCE_SERVER_PORT", REFERENCE_SERVER_PORT)
        self.reference_server_url = f"{scheme}://{host}:{port}"

        datastore_location = MODULE_DIR.parent / "localdata" / "simple_index.db"
        self.database_context = DatabaseContext(str(datastore_location), write_warn_ms=10)
        self.database_context.run_in_thread(sqlite_db.setup)

        self._outbound_data_delivery_event = asyncio.Event()

    async def setup_async(self) -> None:
        self.is_alive = True
        self.aiohttp_session = aiohttp.ClientSession()
        self._outbound_data_delivery_future = asyncio.ensure_future(
            asyncio.create_task(self._attempt_outbound_data_delivery_task()))
        # Futures swallow exceptions so we must install a callback that raises them.
        self._outbound_data_delivery_future.add_done_callback(asyncio_future_callback)

    async def teardown_async(self) -> None:
        self.is_alive = False
        self._outbound_data_delivery_future.cancel()
        await self.aiohttp_session.close()

    def reset_headers_stores(self) -> None:
        if sys.platform == 'win32':
            if os.path.exists(NODE_HEADERS_MMAP_FILEPATH):
                with open(NODE_HEADERS_MMAP_FILEPATH, 'w+') as f:
                    mm = mmap.mmap(f.fileno(), MMAP_SIZE)
                    mm.seek(0)
                    mm.write(b'\00' * mm.size())

            # remove block headers - memory-mapped so need to do it to free memory immediately
            if os.path.exists(LOCAL_HEADERS_MMAP_FILEPATH):
                with open(LOCAL_HEADERS_MMAP_FILEPATH, 'w+') as f:
                    mm = mmap.mmap(f.fileno(), MMAP_SIZE)
                    mm.seek(0)
                    mm.write(b'\00' * mm.size())
        else:
            try:
                if os.path.exists(NODE_HEADERS_MMAP_FILEPATH):
                    os.remove(NODE_HEADERS_MMAP_FILEPATH)
                if os.path.exists(LOCAL_HEADERS_MMAP_FILEPATH):
                    os.remove(LOCAL_HEADERS_MMAP_FILEPATH)
            except FileNotFoundError:
                pass

    def start_threads(self) -> None:
        threading.Thread(target=self.push_notifications_thread, daemon=True).start()

        self.blockchain_state_monitor_thread = Synchronizer(self, self.ws_queue)
        self.blockchain_state_monitor_thread.start()

    def get_ws_clients(self) -> Dict[str, WSClient]:
        with self.ws_clients_lock:
            return self.ws_clients

    def add_ws_client(self, ws_client: WSClient) -> None:
        with self.ws_clients_lock:
            self.ws_clients[ws_client.ws_id] = ws_client

        # The reference server is reconnected pre-emptively allow immediate redelivery.
        self._outbound_data_delivery_event.set()
        self._outbound_data_delivery_event.clear()

    def remove_ws_client_by_id(self, ws_id: str) -> None:
        with self.ws_clients_lock:
            del self.ws_clients[ws_id]

    def push_notifications_thread(self) -> None:
        """Emits any notifications from the queue to all connected websockets"""
        try:
            while self.is_alive:
                try:
                    message_bytes = self.ws_queue.get(timeout=0.5)
                except queue.Empty:
                    continue

                self.logger.debug("Dispatching outgoing websocket message with length=%d",
                    len(message_bytes))
                for ws_client in self.get_ws_clients().values():
                    self.logger.debug("Sending message to websocket, ws_id=%s", ws_client.ws_id)
                    future = asyncio.run_coroutine_threadsafe(
                        ws_client.websocket.send_bytes(message_bytes), self.loop)
                    # Futures swallow exceptions so we must install a callback that raises them.
                    future.add_done_callback(future_callback)
        except Exception:
            self.logger.exception("unexpected exception in push_notifications_thread")
        finally:
            self.logger.info("Exited push notifications thread")

    async def _attempt_outbound_data_delivery_task(self) -> None:
        """
        Non-blocking delivery of new tip filter notifications.
        """
        self.logger.debug("Starting outbound data delivery task")
        MAXIMUM_DELAY = 120.0
        while self.is_alive:
            # No point in trying if there is no reference server connected.
            next_check_delay = MAXIMUM_DELAY
            if len(self.get_ws_clients()) > 0:
                rows = sqlite_db.read_pending_outbound_datas(self.database_context,
                    OutboundDataFlag.NONE, OutboundDataFlag.DISPATCHED_SUCCESSFULLY)
                current_rows = list[OutboundDataRow]()
                if len(rows) > 0:
                    current_time = time.time()
                    for row in rows:
                        if row.date_last_tried + MAXIMUM_DELAY > current_time:
                            next_check_delay = (row.date_last_tried + MAXIMUM_DELAY) \
                                - current_time + 0.5
                            break
                        current_rows.append(row)

                if len(current_rows) > 0:
                    self.logger.debug("Outbound data delivery of %d entries, next delay will "
                        "be %0.2f", len(current_rows), next_check_delay)
                delivery_updates = list[tuple[OutboundDataFlag, int, int]]()
                for row in current_rows:
                    assert row.outbound_data_id is not None
                    url = self.reference_server_url +"/api/v1/tip-filter/matches"
                    headers = {
                        "Content-Type":     "application/json",
                    }
                    batch_text = row.outbound_data.decode("utf-8")
                    updated_flags = row.outbound_data_flags
                    try:
                        async with self.aiohttp_session.post(url, headers=headers,
                                data=batch_text) as response:
                            if response.status == HTTPStatus.OK:
                                self.logger.debug("Posted outbound data to reference server "+
                                    "status=%s, reason=%s", response.status, response.reason)
                                updated_flags |= OutboundDataFlag.DISPATCHED_SUCCESSFULLY
                            else:
                                self.logger.error("Failed to post outbound data to reference "+
                                    "server status=%s, reason=%s", response.status, response.reason)
                    except aiohttp.ClientError:
                        self.logger.exception("Failed to post outbound data to reference server")

                    delivery_updates.append((updated_flags, int(time.time()), row.outbound_data_id))

                if len(delivery_updates) > 0:
                    self.database_context.run_in_thread(
                        sqlite_db.update_outbound_data_last_tried_write, delivery_updates)
            else:
                self.logger.debug("Outbound data delivery deferred due to lack of reference "
                    "server connection")

            try:
                await asyncio.wait_for(self._outbound_data_delivery_event.wait(), next_check_delay)
            except asyncio.TimeoutError:
                pass

    def dispatch_tip_filter_notifications(self, matches: list[PushDataRow],
            block_hash: Optional[bytes]) -> None:
        """
        Non-blocking delivery of new tip filter notifications.
        """
        self.logger.debug("Starting task for dispatching tip filter notifications (%d)",
            len(matches))
        future = asyncio.run_coroutine_threadsafe(
            self.dispatch_tip_filter_notifications_async(matches, block_hash), self.loop)
        # Futures swallow exceptions so we must install a callback that raises any errors.
        future.add_done_callback(future_callback)

    async def dispatch_tip_filter_notifications_async(self, matches: list[PushDataRow],
            block_hash: Optional[bytes]) -> None:
        """
        Worker task for delivery of new tip filter notifications.
        """
        self.logger.debug("Entered task for dispatching tip filter notifications")
        matches_by_hash = dict[bytes, list[PushDataRow]]()
        for match in matches:
            if match.pushdata_hash in matches_by_hash:
                matches_by_hash[match.pushdata_hash].append(match)
            else:
                matches_by_hash[match.pushdata_hash] = [ match ]

        # Get all the accounts and which pushdata they have registered.
        rows = sqlite_db.read_indexer_filtering_registrations_for_notifications(
            self.database_context, list(matches_by_hash))
        self.logger.debug("Found %d registrations for tip filter notifications", len(rows))

        if len(rows) == 0:
            return

        # This also allows us to identify the false positive matches. This is not critical and
        # just for debugging/interest.
        invalid_pushdata_hashes = set(matches_by_hash) - set(row.pushdata_hash for row in rows)
        self.logger.debug("Ignored %d false positive filter matches", len(invalid_pushdata_hashes))

        # Gather the true matches for each account so that we can notify them of those matches.
        matches_by_account_id = dict[int, list[PushDataRow]]()
        for row in rows:
            matched_rows = matches_by_hash[row.pushdata_hash]
            if row.account_id in matches_by_account_id:
                matches_by_account_id[row.account_id].extend(matched_rows)
            else:
                matches_by_account_id[row.account_id] = list(matched_rows)

        # TODO(1.4.0) Servers. Consider moving the callback metadata and the notifications made to
        #     it to the reference server. It can queue the results if they were not able to be
        #     delivered.
        metadata_by_account_id = { row.account_id: row for row
            in sqlite_db.read_account_metadata(self.database_context, list(matches_by_account_id)) }
        block_id = hash_to_hex_str(block_hash) if block_hash is not None else None

        entries = list[TipFilterNotificationEntry]()
        for account_id, matched_rows in matches_by_account_id.items():
            if account_id not in metadata_by_account_id:
                self.logger.error("Account does not have peer channel callback set, "
                    "account_id: %d for hashes: %s", account_id,
                    [ pdh.pushdata_hash.hex() for pdh in matched_rows ])
                continue

            account_metadata = metadata_by_account_id[account_id]
            self.logger.debug("Posting matches for peer channel account %s", account_metadata)
            request_data: TipFilterNotificationEntry = {
                "accountId": account_metadata.external_account_id,
                "matches": [
                    {
                        "pushDataHashHex": matched_row.pushdata_hash.hex(),
                        "transactionId": hash_to_hex_str(matched_row.tx_hash),
                        "transactionIndex": matched_row.idx,
                        "flags": sqlite_db.get_pushdata_match_flag(matched_row.ref_type),
                    } for matched_row in matched_rows
                ]
            }
            entries.append(request_data)

        batch: TipFilterNotificationBatch = {
            "blockId": block_id,
            "entries": entries,
        }

        url = self.reference_server_url +"/api/v1/tip-filter/matches"
        headers = {
            "Content-Type":     "application/json",
        }
        try:
            async with self.aiohttp_session.post(url, headers=headers, json=batch) as response:
                if response.status == HTTPStatus.OK:
                    self.logger.debug("Posted outbound data to reference server "+
                        "status=%s, reason=%s", response.status, response.reason)
                    return

                self.logger.error("Failed to post outbound data to reference server "+
                    "status=%s, reason=%s", response.status, response.reason)
        except aiohttp.ClientError:
            self.logger.exception("Failed to post outbound data to reference server")

        batch_json = json.dumps(batch)
        date_created = int(time.time())
        creation_row = OutboundDataRow(None, batch_json.encode(),
            OutboundDataFlag.TIP_FILTER_NOTIFICATIONS, date_created, date_created)
        self.database_context.run_in_thread(sqlite_db.create_outbound_data_write, creation_row)


# def handle_asyncio_exception(loop: asyncio.AbstractEventLoop, context: dict[str, Any]) -> None:
#     exception = context.get("exception")
#     if exception is not None:
#         logger.exception("Exception raised in asyncio loop", exc_info=exception)
#     else:
#         logger.error("Error in asyncio loop without exception, message: %s", context["message"])


def get_aiohttp_app() -> web.Application:
    loop = asyncio.get_event_loop()
    # loop.set_exception_handler(handle_asyncio_exception)
    app = web.Application()
    app_state = ApplicationState(app, loop)

    # This is the standard aiohttp way of managing state within the handlers
    app['app_state'] = app_state
    app['ws_clients'] = app_state.ws_clients
    app.add_routes([
        web.get("/", handlers.ping),
        web.get("/error", handlers.error),
        web.view("/ws", SimpleIndexerWebSocket),

        web.get("/api/v1/endpoints", handlers.get_endpoints_data),

        # These need to be registered before "/transaction/{txid}" to avoid clashes.
        web.post("/api/v1/transaction/filter", handlers.indexer_post_transaction_filter),
        web.post("/api/v1/transaction/filter:delete",
            handlers.indexer_post_transaction_filter_delete),
        # TODO(1.4.0) Technical debt. We can enforce txid with {txid:[a-fA-F0-9]{64}} in theory.
        web.get("/api/v1/transaction/{txid}", handlers.get_transaction),

        # TODO(1.4.0) Technical debt. We can enforce txid with {txid:[a-fA-F0-9]{64}} in theory.
        web.get("/api/v1/merkle-proof/{txid}", handlers.get_merkle_proof),
        web.post("/api/v1/restoration/search", handlers.get_restoration_matches),

        web.post("/api/v1/output-spend", handlers.post_output_spends),
        web.post("/api/v1/output-spend/notifications",
            handlers.post_output_spend_notifications_register),
        web.post("/api/v1/output-spend/notifications:unregister",
            handlers.post_output_spend_notifications_unregister),
    ])
    return app


if __name__ == "__main__":
    app = get_aiohttp_app()
    web.run_app(app, host=SERVER_HOST, port=SERVER_PORT)
