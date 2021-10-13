import aiohttp
from aiohttp import web
import json
import logging
import uuid


class WSClient(object):

    def __init__(self, ws_id: str, websocket: web.WebSocketResponse):
        self.ws_id = ws_id
        self.websocket = websocket


class SimpleIndexerWebSocket(web.View):
    logger = logging.getLogger("websocket")

    async def get(self):
        ws = web.WebSocketResponse()
        await ws.prepare(self.request)
        ws_id = str(uuid.uuid4())

        try:
            client = WSClient(ws_id=ws_id, websocket=ws)
            self.request.app['app_state'].add_ws_client(client)
            self.logger.debug('%s connected. host=%s.', client.ws_id, self.request.host)
            await self._handle_new_connection(client)
            return ws
        finally:
            await ws.close()
            self.logger.debug("removing websocket id: %s", ws_id)
            del self.request.app['ws_clients'][ws_id]

    async def _handle_new_connection(self, client):
        self.ws_clients = self.request.app['ws_clients']

        async for msg in client.websocket:
            if msg.type == aiohttp.WSMsgType.text:
                self.logger.debug('%s client sent: %s', client.ws_id, msg.data)
                request_json = json.loads(msg.data)

                msg_type = request_json['msg_type']
                if msg_type == 'test':
                    self.logger.debug(
                        f"Got test command from websocket id: {client.ws_id}; data: {request_json}")
                # if msg_type == 'some_other_command':
                #    pass

                response_json = json.dumps(request_json)
                await client.websocket.send_str(response_json)

            elif msg.type == aiohttp.WSMsgType.error:
                # 'client.websocket.exception()' merely returns ClientWebSocketResponse._exception
                # without a traceback. see aiohttp.ws_client.py:receive for details.
                self.logger.error('ws connection closed with exception %s',
                    client.websocket.exception())
