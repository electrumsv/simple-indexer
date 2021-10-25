import json
import typing

import aiohttp
from aiohttp import web
import logging

from simple_indexer.types import RestorationFilterRequest

if typing.TYPE_CHECKING:
    from simple_indexer.server import ApplicationState
    from simple_indexer.sqlite_db import SQLiteDatabase


logger = logging.getLogger('handlers')


async def ping(request: web.Request) -> web.Response:
    return web.Response(text="true")


async def error(request: web.Request) -> web.Response:
    raise ValueError("This is a test of raising an exception in the handler")


async def get_pushdata_filter_matches(request: web.Request):
    """This the main endpoint for the rapid restoration API"""
    app_state: 'ApplicationState' = request.app['app_state']
    sqlite_db: SQLiteDatabase = app_state.sqlite_db
    body = await request.content.read()
    if body:
        pushdata_hashes: RestorationFilterRequest = json.loads(body.decode('utf-8'))['filterKeys']
    else:
        return web.json_response({'error': 'no pushdata hashes submitted in request body'})

    headers = {
        'Content-Type': 'application/json',
        'User-Agent': 'SimpleIndexer'
    }
    response = aiohttp.web.StreamResponse(status=200, reason='OK', headers=headers)
    await response.prepare(request)

    result = sqlite_db.get_pushdata_filter_matches(pushdata_hashes)
    for match in result:
        row = (json.dumps(match) + "\n").encode('utf-8')
        await response.write(row)

    await response.write_eof()
    return response
