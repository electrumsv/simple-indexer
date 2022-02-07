from __future__ import annotations
from datetime import datetime, timedelta
import json
from typing import Any, cast, Dict, Optional, TYPE_CHECKING

import aiohttp
import requests
from aiohttp import web
import logging

from bitcoinx import hex_str_to_hash, hash_to_hex_str
from electrumsv_node import electrumsv_node

from .constants import SERVER_HOST, SERVER_PORT
from .types import FILTER_RESPONSE_SIZE, filter_response_struct, outpoint_struct, \
    OutpointJSONType, output_spend_struct, OutpointType, RestorationFilterRequest, \
    tsc_merkle_proof_json_to_binary, ZEROED_OUTPOINT

if TYPE_CHECKING:
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
                "apiType": "bsvapi.output-spend",
                "apiVersion": 1,
                "baseURL": "/api/v1/output-spend",
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
    app_state: ApplicationState = request.app['app_state']
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
    app_state: ApplicationState = request.app['app_state']
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
    """
    It is expected that a valid reponse will have a content length, and should stream the data
    if possible. This is to allow things like 4 GiB transactions to be provided within proof
    with no server overhead over providing both proof and transaction separately.

    This regtest implementation has to use the node to provide data via the JSON-RPC API and this
    will never be streamable or scalable. But professional services would be expected to design
    for streaming out of the box, and would not be encumbered by limitations imposed by the node.
    """
    # Todo - use the bitcoin node as much as possible (this is only for RegTest)
    app_state: ApplicationState = request.app['app_state']
    sqlite_db: SQLiteDatabase = app_state.sqlite_db
    accept_type = request.headers.get('Accept')

    txid = request.match_info['txid']
    if not txid:
        return web.Response(status=400, reason="no txid submitted")

    block_hash = sqlite_db.get_block_hash_for_tx(hex_str_to_hash(txid))
    if not block_hash:
        return web.Response(status=404)

    include_full_tx = request.query.get("includeFullTx") == "1"
    target_type = request.query.get("targetType", "hash")
    if target_type is not None and target_type not in {'hash', 'header', 'merkleroot'}:
        return web.Response(status=400)

    # Request TSC merkle proof from the node
    # Todo - binary format not currently supported by the node
    try:
        tsc_merkle_proof = electrumsv_node.call_any("getmerkleproof2",
            hash_to_hex_str(block_hash), txid, include_full_tx, target_type).json()['result']

        if accept_type == 'application/octet-stream':
            binary_response = tsc_merkle_proof_json_to_binary(tsc_merkle_proof,
                include_full_tx=include_full_tx, target_type=target_type)
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


async def post_output_spends(request: web.Request) -> web.Response:
    """
    Return the metadata for each provided outpoint if they are spent.
    """
    accept_type = request.headers.get('Accept')
    content_type = request.headers.get('Content-Type')
    body = await request.content.read()
    if not body:
        raise web.HTTPBadRequest(reason="no body")

    client_outpoints: list[OutpointType] = []
    if content_type == 'application/json':
        # Convert the incoming JSON representation to the internal binary representation.
        client_outpoints_json: list[OutpointJSONType] = json.loads(body.decode('utf-8'))
        if not isinstance(client_outpoints_json, list):
            raise web.HTTPBadRequest(reason="payload is not a list")
        for entry in client_outpoints_json:
            if not isinstance(entry, list) or len(entry) != 2 or not isinstance(entry[1], int):
                raise web.HTTPBadRequest(reason="one or more payload entries are incorrect")
            try:
                tx_hash = hex_str_to_hash(entry[0])
            except (ValueError, TypeError):
                raise web.HTTPBadRequest(reason="one or more payload entries are incorrect")
            client_outpoints.append((tx_hash, entry[1]))
    elif content_type == 'application/octet-stream':
        raise web.HTTPBadRequest(reason="binary request body support not implemented yet")
    else:
        raise web.HTTPBadRequest(reason="unknown request body content type")

    app_state: ApplicationState = request.app['app_state']
    sqlite_db: SQLiteDatabase = app_state.sqlite_db
    existing_rows = sqlite_db.get_spent_outpoints(client_outpoints)

    if accept_type == 'application/octet-stream':
        result_bytes = b""
        for row in existing_rows:
            result_bytes += output_spend_struct.pack(row.out_tx_hash, row.out_idx,
                row.in_tx_hash, row.in_idx, row.block_hash if row.block_hash else bytes(32))
        return web.Response(body=result_bytes)
    else:
        json_list: list[tuple[str, int, str, int, Optional[str]]] = []
        for row in existing_rows:
            json_list.append((hash_to_hex_str(row.out_tx_hash), row.out_idx,
                hash_to_hex_str(row.in_tx_hash), row.in_idx,
                row.block_hash.hex() if row.block_hash else None))
        return web.json_response(data=json_list)


async def post_output_spend_notifications_register(request: web.Request) -> web.Response:
    """
    Register the caller provided UTXO references so that we send notifications if they get
    spent. We also return the current state for any that are known as a response.

    This is a bit clumsy, but this is the simple indexer and it is intended to be the minimum
    effort to allow ElectrumSV to be used against regtest. It is expected that the caller
    has connected to the notification web socket before making this call, and can keep up
    with the notifications.
    """
    accept_type = request.headers.get('Accept')
    content_type = request.headers.get('Content-Type')
    body = await request.content.read()
    if not body:
        raise web.HTTPBadRequest(reason="no body")

    client_outpoints: list[OutpointType] = []
    if content_type == 'application/json':
        # Convert the incoming JSON representation to the internal binary representation.
        client_outpoints_json: list[OutpointJSONType] = json.loads(body.decode('utf-8'))
        if not isinstance(client_outpoints_json, list):
            raise web.HTTPBadRequest(reason="payload is not a list")
        for entry in client_outpoints_json:
            if not isinstance(entry, list) or len(entry) != 2 or not isinstance(entry[1], int):
                raise web.HTTPBadRequest(reason="one or more payload entries are incorrect")
            try:
                tx_hash = hex_str_to_hash(entry[0])
            except (ValueError, TypeError):
                raise web.HTTPBadRequest(reason="one or more payload entries are incorrect")
            client_outpoints.append((tx_hash, entry[1]))
    elif content_type == 'application/octet-stream':
        if len(body) % outpoint_struct.size != 0:
            raise web.HTTPBadRequest(reason="binary request body malformed")

        for outpoint_index in range(len(body) // outpoint_struct.size):
            outpoint = cast(OutpointType,
                outpoint_struct.unpack_from(body, outpoint_index * outpoint_struct.size))
            client_outpoints.append(outpoint)
    else:
        raise web.HTTPBadRequest(reason="unknown request body content type")

    app_state: ApplicationState = request.app['app_state']
    synchronizer = app_state.blockchain_state_monitor_thread
    if synchronizer is None:
        raise web.HTTPInternalServerError(reason="error finding synchronizer")

    existing_rows = synchronizer.register_output_spend_notifications(client_outpoints)

    if accept_type == 'application/octet-stream':
        result_bytes = b""
        for row in existing_rows:
            result_bytes += output_spend_struct.pack(row.out_tx_hash, row.out_idx,
                row.in_tx_hash, row.in_idx, row.block_hash if row.block_hash else bytes(32))
        return web.Response(body=result_bytes)
    else:
        json_list: list[tuple[str, int, str, int, Optional[str]]] = []
        for row in existing_rows:
            json_list.append((hash_to_hex_str(row.out_tx_hash), row.out_idx,
                hash_to_hex_str(row.in_tx_hash), row.in_idx,
                row.block_hash.hex() if row.block_hash else None))
        return web.json_response(data=json.dumps(json_list))


async def post_output_spend_notifications_unregister(request: web.Request) -> web.Response:
    """
    This provides a way for the monitored output spends to be unregistered or cleared. It is
    assumed that whomever has access to this endpoint, has control over the registration and
    can do this on behalf of all users.

    The reference server manages who is subscribed to what, and what should be monitored, and
    uses this method to ensure the simple indexer is only monitoring what it needs to.

    If the reference server wishes to clear all monitored output spends, it should send one
    outpoint and it should be zeroed (null tx hash and zero index).
    """
    content_type = request.headers.get('Content-Type')
    body = await request.content.read()
    if not body:
        raise web.HTTPBadRequest(reason="no body")

    client_outpoints: list[OutpointType] = []
    if content_type == 'application/octet-stream':
        if len(body) % outpoint_struct.size != 0:
            raise web.HTTPBadRequest(reason="binary request body malformed")

        for outpoint_index in range(len(body) // outpoint_struct.size):
            outpoint = cast(OutpointType,
                outpoint_struct.unpack_from(body, outpoint_index * outpoint_struct.size))
            client_outpoints.append(outpoint)
    else:
        raise web.HTTPBadRequest(reason="unknown request body content type")

    app_state: ApplicationState = request.app['app_state']
    synchronizer = app_state.blockchain_state_monitor_thread
    if synchronizer is None:
        raise web.HTTPInternalServerError(reason="error finding synchronizer")

    if len(client_outpoints) == 1 and client_outpoints[0] == ZEROED_OUTPOINT:
        synchronizer.clear_output_spend_notifications()
    else:
        synchronizer.unregister_output_spend_notifications(client_outpoints)
    return web.Response()
