import asyncio
import logging
import sys

import traceback
import aiohttp
from aiohttp import ClientConnectorError

SERVER_HOST = '127.0.0.1'
SERVER_PORT = 49241
BASE_URL = f"http://{SERVER_HOST}:{SERVER_PORT}"
WS_URL = "http://localhost:49241/ws"
GET_STATUS_URL = BASE_URL + '/api/get_status'


class MockApplicationState:

    def __init__(self) -> None:
        # some state
        pass


class WebsocketClient:
    def __init__(self, app_state: MockApplicationState) -> None:
        self.app_state = app_state
        self.logger = logging.getLogger("websocket-client")

    async def subscribe(self) -> None:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(WS_URL, timeout=5.0) as ws:
                print(f'Connected to {WS_URL}')

                async for msg in ws:
                    print('Message received from server:', msg)
                    if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break


# entrypoint to main event loop
async def main() -> None:
    app_state = MockApplicationState()
    client = WebsocketClient(app_state)
    while True:
        try:
            await client.subscribe()  # using aiohttp
        except (ConnectionRefusedError, ClientConnectorError):
            print(f"Unable to connect to: {WS_URL} - retrying...")
        except Exception as e:
            exc_type, exc_value, exc_tb = sys.exc_info()
            tb = traceback.TracebackException(exc_type, exc_value, exc_tb)
            print(''.join(tb.format_exception_only()))


if __name__ == "__main__":
    asyncio.run(main())
