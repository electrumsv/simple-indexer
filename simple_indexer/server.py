from pathlib import Path

from aiohttp import web
import asyncio
import mmap
import os
import sys
import logging
import queue
import threading
from typing import Dict, Optional

from bitcoinx import (
    CheckPoint,
    BitcoinRegtest,
    Headers,
)

from .constants import SERVER_HOST, SERVER_PORT
from .handlers_ws import SimpleIndexerWebSocket, WSClient
from . import handlers
from .synchronizer import Synchronizer
from .sqlite_db import SQLiteDatabase


MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))

# Silence verbose logging
aiohttp_logger = logging.getLogger("aiohttp")
aiohttp_logger.setLevel(logging.WARNING)


NODE_HEADERS_MMAP_FILEPATH = MODULE_DIR.parent.joinpath('node_headers.mmap')
LOCAL_HEADERS_MMAP_FILEPATH = MODULE_DIR.parent.joinpath('local_headers.mmap')
MMAP_SIZE = 100_000  # headers count - should be ample for RegTest
CHECKPOINT = CheckPoint(
    bytes.fromhex(
        "010000000000000000000000000000000000000000000000000000000000000000000000"
        "3ba3edfd7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa4b1e5e4adae5494d"
        "ffff7f2002000000"), height=0, prev_work=0
)


class ApplicationState(object):

    def __init__(self, app: web.Application, loop: asyncio.AbstractEventLoop) -> None:
        self.logger = logging.getLogger('app_state')
        self.app = app
        self.loop = loop

        if int(os.getenv('SIMPLE_INDEX_RESET', "1")):
            self.reset_headers_stores()

        self.node_headers = Headers(BitcoinRegtest, NODE_HEADERS_MMAP_FILEPATH, CHECKPOINT)
        self.local_headers = Headers(BitcoinRegtest, LOCAL_HEADERS_MMAP_FILEPATH, CHECKPOINT)

        self.ws_clients: Dict[str, WSClient] = {}
        self.ws_clients_lock: threading.RLock = threading.RLock()

        self.ws_queue: queue.Queue[bytes] = queue.Queue()
        self.blockchain_state_monitor_thread: Optional[Synchronizer] = None
        self.sqlite_db = SQLiteDatabase(MODULE_DIR.parent / 'simple_index.db')

    def reset_headers_stores(self):
        if sys.platform == 'win32':
            if os.path.exists(NODE_HEADERS_MMAP_FILEPATH):
                with open(NODE_HEADERS_MMAP_FILEPATH, 'w+') as f:
                    mm = mmap.mmap(f.fileno(), MMAP_SIZE)
                    mm.seek(0)
                    mm.write(b'\00' * mm.size())

            # remove block headers - memory-mapped so need to do it this way to free memory immediately
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

    def start_threads(self):
        threading.Thread(target=self.push_notifications_thread, daemon=True).start()

        self.blockchain_state_monitor_thread = Synchronizer(self, self.ws_queue)
        self.blockchain_state_monitor_thread.start()

    def get_ws_clients(self) -> Dict[str, WSClient]:
        with self.ws_clients_lock:
            return self.ws_clients

    def add_ws_client(self, ws_client: WSClient):
        with self.ws_clients_lock:
            self.ws_clients[ws_client.ws_id] = ws_client

    def remove_ws_client_by_id(self, ws_id: str) -> None:
        with self.ws_clients_lock:
            del self.ws_clients[ws_id]

    def push_notifications_thread(self) -> None:
        """Emits any notifications from the queue to all connected websockets"""
        try:
            while self.app.is_alive:
                try:
                    message_bytes = self.ws_queue.get(timeout=0.5)
                except queue.Empty:
                    continue

                self.logger.debug("Dispatching outgoing websocket message with length=%d",
                    len(message_bytes))
                for ws_client in self.get_ws_clients().values():
                    self.logger.debug("Sending message to websocket, ws_id=%s", ws_client.ws_id)
                    asyncio.run_coroutine_threadsafe(ws_client.websocket.send_bytes(message_bytes),
                        self.loop)
        except Exception:
            self.logger.exception("unexpected exception in push_notifications_thread")
        finally:
            self.logger.info("Closing push notifications thread")


def get_aiohttp_app() -> web.Application:
    loop = asyncio.get_event_loop()
    app = web.Application()
    app_state = ApplicationState(app, loop)

    # This is the standard aiohttp way of managing state within the handlers
    app['app_state'] = app_state
    app['ws_clients'] = app_state.ws_clients
    app.add_routes([
        web.get("/", handlers.ping),
        web.get("/error", handlers.error),
        web.get("/api/v1/endpoints", handlers.get_endpoints_data),
        web.post("/api/v1/restoration/search", handlers.get_pushdata_filter_matches),
        web.get("/api/v1/transaction/{txid}", handlers.get_transaction),
        web.get("/api/v1/merkle-proof/{txid}", handlers.get_merkle_proof),
        web.post("/api/v1/output-spend", handlers.post_output_spends),
        web.post("/api/v1/output-spend/notifications",
            handlers.post_output_spend_notifications_register),
        web.post("/api/v1/output-spend/notifications:unregister",
            handlers.post_output_spend_notifications_unregister),
        web.view("/ws", SimpleIndexerWebSocket), ])
    return app


if __name__ == "__main__":
    app = get_aiohttp_app()
    web.run_app(app, host=SERVER_HOST, port=SERVER_PORT)
