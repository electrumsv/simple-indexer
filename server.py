"""
NOTE: This server's sole purpose in life is to expose the APIs that ElectrumSV needs in future
on RegTest. It is not in any way designed with scalability in mind. It is designed to be simple,
the code easy to read and understand and above all else correct.

This will be used for functional testing of ElectrumSV but the secondary motive is that other
application developers can test against this set of APIs. After all the problems that ElectrumSV
needs to solve will be shared by many other SPV applications.
"""

import os
import sys
from pathlib import Path
import asyncio
import logging
import typing
from logging.handlers import RotatingFileHandler
from typing import Optional

from aiohttp import web

if typing.TYPE_CHECKING:
    from .simple_indexer.server import ApplicationState


from simple_indexer.constants import SERVER_HOST, SERVER_PORT
from simple_indexer.server import get_aiohttp_app


MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
FULL_LOG_PATH = MODULE_DIR / 'logs' / 'simple_indexer.log'
logger = logging.getLogger("server")


def create_log_file_if_not_exist():
    if not Path(FULL_LOG_PATH).exists():
        os.makedirs(os.path.dirname(FULL_LOG_PATH), exist_ok=True)
        with open(FULL_LOG_PATH, 'w') as f:
            f.write('')


def setup_logging():
    create_log_file_if_not_exist()
    logging.basicConfig(format='%(asctime)s %(levelname)-8s %(name)-24s %(message)s',
        level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')
    file_handler = RotatingFileHandler(FULL_LOG_PATH, mode='w', backupCount=1, encoding='utf-8')
    formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(name)-24s %(message)s')
    file_handler.setFormatter(formatter)
    logging.root.addHandler(file_handler)



class AiohttpServer:

    def __init__(self, app: web.Application, host: str = SERVER_HOST,
        port: int = SERVER_PORT) -> None:
        self.runner: Optional[web.AppRunner] = None
        self.app = app
        self.app_state: 'ApplicationState' = app['app_state']
        self.app.on_startup.append(self.on_startup)
        self.app.on_shutdown.append(self.on_shutdown)
        self.app.freeze()  # No further callback modification allowed
        self.host = host
        self.port = port
        self.logger = logging.getLogger("aiohttp-rest-api")

    async def on_startup(self, app: web.Application) -> None:
        self.logger.debug("starting...")
        self.logger.debug(f"file logging path={FULL_LOG_PATH}")

    async def on_shutdown(self, app: web.Application) -> None:
        self.logger.debug("cleaning up...")
        self.app.is_alive = False
        self.logger.debug("stopped.")

    async def start(self) -> None:
        self.app.is_alive = True
        self.logger.debug("started on http://%s:%s", self.host, self.port)
        self.runner = web.AppRunner(self.app, access_log=None)
        await self.runner.setup()
        site = web.TCPSite(self.runner, self.host, self.port, reuse_address=True)
        await site.start()
        self.app_state.start_threads()
        while self.app.is_alive:
            await asyncio.sleep(0.5)

    async def stop(self) -> None:
        assert self.runner is not None
        await self.runner.cleanup()


async def main():
    setup_logging()
    app = get_aiohttp_app()
    server = AiohttpServer(app)
    try:
        await server.start()
    finally:
        await server.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
        sys.exit(0)
    except KeyboardInterrupt:
        logger.debug("Simple Indexer stopped")
    except Exception:
        logger.exception("unexpected exception in __main__")
    finally:
        logger.info("Simple Indexer stopped")
