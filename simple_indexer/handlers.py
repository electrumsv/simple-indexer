import json
import struct
import typing

import aiohttp
from aiohttp import web
import logging

from bitcoinx import hex_str_to_hash

from simple_indexer.types import RestorationFilterRequest, filter_response_struct, \
    FILTER_RESPONSE_SIZE

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
    accept_type = request.headers.get('Accept')

    body = await request.content.read()
    if body:
        pushdata_hashes: RestorationFilterRequest = json.loads(body.decode('utf-8'))['filterKeys']
    else:
        return web.Response(status=400)

    if accept_type == 'application/octet-stream':
        headers = {'Content-Type': 'application/octet-stream', 'User-Agent': 'SimpleIndexer'}
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
    else:
        headers = {'Content-Type': 'application/json', 'User-Agent': 'SimpleIndexer'}
        response = aiohttp.web.StreamResponse(status=200, reason='OK', headers=headers)
        await response.prepare(request)

        result = sqlite_db.get_pushdata_filter_matches(pushdata_hashes, json=True)
        for match in result:
            row = (json.dumps(match) + "\n").encode('utf-8')
            await response.write(row)

    await response.write_eof()
    return response


async def get_transaction(request: web.Request) -> web.Response:
    app_state: 'ApplicationState' = request.app['app_state']
    sqlite_db: SQLiteDatabase = app_state.sqlite_db
    accept_type = request.headers.get('Accept')

    try:
        txid = request.match_info['txid']
        if not txid:
            raise ValueError('no txid submitted')

        rawtx = sqlite_db.get_transaction(hex_str_to_hash(txid))
    except ValueError:
        return web.Response(status=400)

    if accept_type == 'application/octet-stream':
        return web.Response(body=rawtx)
    else:
        return web.json_response(data=rawtx.hex())
