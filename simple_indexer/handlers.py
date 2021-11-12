import json
import struct
import typing

import aiohttp
from aiohttp import web
import logging

from simple_indexer.types import RestorationFilterRequest, RESULT_UNPACK_FORMAT, \
    filter_response_struct, FILTER_RESPONSE_SIZE

if typing.TYPE_CHECKING:
    from simple_indexer.server import ApplicationState
    from simple_indexer.sqlite_db import SQLiteDatabase


logger = logging.getLogger('handlers')


async def ping(request: web.Request) -> web.Response:
    return web.Response(text="true")


async def error(request: web.Request) -> web.Response:
    raise ValueError("This is a test of raising an exception in the handler")


async def get_pushdata_filter_matches_json(request: web.Request):
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

    result = sqlite_db.get_pushdata_filter_matches(pushdata_hashes, json=True)
    for match in result:
        row = (json.dumps(match) + "\n").encode('utf-8')
        await response.write(row)

    await response.write_eof()
    return response


async def get_pushdata_filter_matches_binary(request: web.Request):
    """This the main endpoint for the rapid restoration API"""
    app_state: 'ApplicationState' = request.app['app_state']
    sqlite_db: SQLiteDatabase = app_state.sqlite_db
    body = await request.content.read()
    if body:
        pushdata_hashes: RestorationFilterRequest = json.loads(body.decode('utf-8'))['filterKeys']
    else:
        return web.json_response({'error': 'no pushdata hashes submitted in request body'})

    headers = {
        'Content-Type': 'application/octet-stream',
        'User-Agent': 'SimpleIndexer'
    }
    response = aiohttp.web.StreamResponse(status=200, reason='OK', headers=headers)
    await response.prepare(request)

    result = sqlite_db.get_pushdata_filter_matches(pushdata_hashes, json=False)
    count = 0
    for match in result:
        packed_match = filter_response_struct.pack(*match)
        await response.write(packed_match)
        count += 1

    total_size = count * FILTER_RESPONSE_SIZE
    logger.debug(f"Total pushdata filter match response size: {total_size} for count: {count}")
    await response.write_eof()
    return response
