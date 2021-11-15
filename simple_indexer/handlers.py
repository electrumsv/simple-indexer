from datetime import datetime, timedelta
import json
import struct
import typing
from typing import Any, Dict

import aiohttp
import requests
from aiohttp import web
import logging

from bitcoinx import hex_str_to_hash, hash_to_hex_str
from electrumsv_node import electrumsv_node

from .constants import SERVER_HOST, SERVER_PORT
from .types import RestorationFilterRequest, filter_response_struct, \
    FILTER_RESPONSE_SIZE, tsc_merkle_proof_json_to_binary

if typing.TYPE_CHECKING:
    from .server import ApplicationState
    from .sqlite_db import SQLiteDatabase


logger = logging.getLogger('handlers')


async def ping(request: web.Request) -> web.Response:
    return web.Response(text="true")


async def error(request: web.Request) -> web.Response:
    raise ValueError("This is a test of raising an exception in the handler")


async def get_endpoints_data(request: web.Request) -> web.Response:
    utc_now_datetime = datetime.utcnow()
    utc_expiry_datetime = utc_now_datetime + timedelta(days=1)

    data: Dict[str, Any] = {
        "apiType": "bsvapi.endpoints",
        "apiVersion": 1,
        "baseUrl": f"http://{SERVER_HOST}:{SERVER_PORT}",
        "timestamp": utc_now_datetime.isoformat() +"Z",
        "expiryTime": utc_expiry_datetime.isoformat() +"Z",
        "endpoints": [
            {
                "apiType": "bsvapi.transaction",
                "apiVersion": 1,
                "baseURL": "/api/v1/transaction",
            },
            {
                "apiType": "bsvapi.merkle-proof",
                "apiVersion": 1,
                "baseURL": "/api/v1/merkle-proof",
            },
            {
                "apiType": "bsvapi.restoration",
                "apiVersion": 1,
                "baseURL": "/api/v1/restoration",
                "pricing": {
                    "data": {
                        "satoshis": 4524,
                        "bytes": 10000000,
                    }
                }
            }
        ]
    }
    return web.json_response(data=data)


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

        finalization_flag = b'\x00'
        await response.write(finalization_flag)
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
        if not rawtx:
            return web.Response(status=404)
    except ValueError:
        return web.Response(status=400)

    if accept_type == 'application/octet-stream':
        return web.Response(body=rawtx)
    else:
        return web.json_response(data=rawtx.hex())


async def get_merkle_proof(request: web.Request) -> web.Response:
    # Todo - use the bitcoin node as much as possible (this is only for RegTest)
    app_state: 'ApplicationState' = request.app['app_state']
    sqlite_db: SQLiteDatabase = app_state.sqlite_db
    accept_type = request.headers.get('Accept')

    # Get block_hash
    try:
        txid = request.match_info['txid']
        if not txid:
            raise ValueError('no txid submitted')

        block_hash = sqlite_db.get_block_hash_for_tx(hex_str_to_hash(txid))
        if not block_hash:
            return web.Response(status=404)
    except ValueError:
        return web.Response(status=400)

    include_full_tx = False
    target_type = 'hash'
    body = await request.content.read()
    if body:
        json_body = json.loads(body.decode('utf-8'))
        include_full_tx = json_body.get('includeFullTx')
        target_type = json_body.get('targetType')
        if include_full_tx is not None and include_full_tx not in {True, False}:
            return web.Response(status=400)

        if target_type is not None and target_type not in {'hash', 'header', 'merkleroot'}:
            return web.Response(status=400)

    # Request TSC merkle proof from the node
    # Todo - binary format not currently supported by the node
    try:
        tsc_merkle_proof = electrumsv_node.call_any("getmerkleproof2",
            hash_to_hex_str(block_hash), txid, include_full_tx, target_type).json()['result']

        if accept_type == 'application/octet-stream':
            binary_response = tsc_merkle_proof_json_to_binary(tsc_merkle_proof,
                include_full_tx=include_full_tx,
                target_type=target_type)
            return web.Response(body=binary_response)
        else:
            return web.json_response(data=tsc_merkle_proof)
    except requests.exceptions.HTTPError as e:
        # the node does not return merkle proofs when there is only a single coinbase tx
        # in the block. It could be argued that this is a bug and it should return the same format.
        result = electrumsv_node.call_any("getrawtransaction", txid, 1).json()['result']
        rawtx = result['hex']
        blockhash = result['blockhash']
        result = electrumsv_node.call_any("getblock", blockhash).json()['result']
        num_tx = result['num_tx']
        if num_tx != 1:
            return web.Response(status=404)
        else:
            merkleroot = result['merkleroot']
            assert merkleroot == txid

            txOrId = txid
            if include_full_tx:
                txOrId = rawtx

            if target_type == 'hash':
                target = blockhash
            elif target_type == 'header':
                target = electrumsv_node.call_any("getblockheader", blockhash, False).json()['result']
            elif target_type == 'merkleroot':
                target = merkleroot
            else:
                target = blockhash

            tsc_merkle_proof = {
                'index': 0,
                'txOrId': txOrId,
                'target': target,
                'nodes': []
            }
            if accept_type == 'application/octet-stream':
                binary_response = tsc_merkle_proof_json_to_binary(tsc_merkle_proof,
                    include_full_tx=include_full_tx, target_type=target_type)
                return web.Response(body=binary_response)
            else:
                return web.json_response(data=tsc_merkle_proof)
