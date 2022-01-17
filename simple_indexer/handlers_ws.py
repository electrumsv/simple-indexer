from __future__ import annotations
import logging
from typing import TYPE_CHECKING
import uuid

from aiohttp import web, WSMsgType

if TYPE_CHECKING:
    from .server import ApplicationState


class WSClient(object):
    def __init__(self, ws_id: str, websocket: web.WebSocketResponse):
        self.ws_id = ws_id
        self.websocket = websocket


class SimpleIndexerWebSocket(web.View):
    logger = logging.getLogger("websocket")

    async def get(self) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(self.request)
        ws_id = str(uuid.uuid4())

        app_state: ApplicationState = self.request.app['app_state']
        try:
            client = WSClient(ws_id=ws_id, websocket=ws)
            app_state.add_ws_client(client)
            self.logger.debug('%s connected, host=%s', client.ws_id, self.request.host)

            async for message in client.websocket:
                if message.type == WSMsgType.ERROR:
                    self.logger.error("websocket error", exc_info=message.data)
                else:
                    self.logger.error("websocket exiting on unwanted incoming message %s",
                        message)
                    break

            return ws
        finally:
            await ws.close()
            self.logger.debug("removing websocket id: %s", ws_id)
            app_state.remove_ws_client_by_id(ws_id)
