from __future__ import annotations
from datetime import datetime, timedelta
import json
from typing import Any, cast, Dict, Optional, TYPE_CHECKING

import requests
from aiohttp import web
import logging

from bitcoinx import hex_str_to_hash, hash_to_hex_str
from electrumsv_node import electrumsv_node

from .constants import SERVER_HOST, SERVER_PORT
from . import sqlite_db
from .types import FILTER_RESPONSE_SIZE, filter_response_struct, IndexerPushdataRegistrationFlag, \
    outpoint_struct, OutpointJSONType, output_spend_struct, OutpointType, \
    RestorationFilterRequest, tip_filter_entry_struct, TipFilterRegistrationEntry, \
    TipFilterRegistrationResponse, tsc_merkle_proof_json_to_binary, ZEROED_OUTPOINT

if TYPE_CHECKING:
    from .server import ApplicationState


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


async def indexer_get_indexer_settings(request: web.Request) -> web.Response:
    app_state: ApplicationState = request.app['app_state']
    accept_type = request.headers.get("Accept", "*/*")
    if accept_type == "*/*":
        accept_type = "application/json"
    if accept_type != "application/json":
        raise web.HTTPBadRequest(reason="invalid 'Accept', expected 'application/json', "
            f"got '{accept_type}'")

    account_id_text = request.query.get("account_id")
    if account_id_text is None:
        raise web.HTTPBadRequest(reason="missing 'account_id' query parameter")
    try:
        external_account_id = int(account_id_text)
    except ValueError:
        raise web.HTTPBadRequest(reason="invalid 'account_id' query parameter")

    settings_object = sqlite_db.get_indexer_settings_by_external_id(app_state.database_context,
        external_account_id)
    if settings_object is None:
        # The default values.
        settings_object = {
            "tipFilterCallbackUrl": None,
        }
    return web.json_response(data=settings_object)


async def indexer_post_indexer_settings(request: web.Request) -> web.Response:
    """
    This updates all the values that are present, it does not touch those that are not.
    """
    app_state: ApplicationState = request.app['app_state']

    content_type = request.headers.get('Content-Type', 'application/json')
    if content_type != 'application/json':
        raise web.HTTPBadRequest(reason="invalid 'Content-Type', expected 'application/json', "
            f"got '{content_type}'")

    accept_type = request.headers.get('Accept', 'application/json')
    if accept_type not in ('*/*', 'application/json'):
        raise web.HTTPBadRequest(reason="invalid 'Accept', expected 'application/json', "
            f"got '{accept_type}'")

    account_id_text = request.query.get("account_id")
    if account_id_text is None:
        raise web.HTTPBadRequest(reason="missing 'account_id' query parameter")
    try:
        external_account_id = int(account_id_text)
    except ValueError:
        raise web.HTTPBadRequest(reason="invalid 'account_id' query parameter")

    settings_update_object =  await request.json()
    if not isinstance(settings_update_object, dict):
        raise web.HTTPBadRequest(reason="invalid settings update object in body")

    settings_object = await app_state.database_context.run_in_thread_async(
        sqlite_db.set_indexer_settings_by_external_id_write, external_account_id,
            settings_update_object)
    return web.json_response(data=settings_object)


async def get_restoration_matches(request: web.Request) -> web.StreamResponse:
    """This the main endpoint for the rapid restoration API"""
    app_state: ApplicationState = request.app['app_state']
    accept_type = request.headers.get("Accept", "*/*")
    if accept_type == "*/*":
        accept_type = "application/json"

    body = await request.content.read()
    if body:
        pushdata_hashes_hex: RestorationFilterRequest = \
            json.loads(body.decode('utf-8'))['filterKeys']
    else:
        return web.Response(status=400)

    pushdata_hashes = [ bytes.fromhex(value) for value in pushdata_hashes_hex ]

    if accept_type == 'application/octet-stream':
        headers = {'Content-Type': 'application/octet-stream', 'User-Agent': 'SimpleIndexer'}
        response = web.StreamResponse(status=200, reason='OK', headers=headers)
        await response.prepare(request)

        result = sqlite_db.get_restoration_matches(app_state.database_context, pushdata_hashes,
            json=False)

        count = 0
        for match in result:
            packed_match = filter_response_struct.pack(*match)
            await response.write(packed_match)
            count += 1

        total_size = count * FILTER_RESPONSE_SIZE
        logger.debug(f"Total pushdata filter match response size: {total_size} for count: {count}")
    else:
        headers = {'Content-Type': 'application/json', 'User-Agent': 'SimpleIndexer'}
        response = web.StreamResponse(status=200, reason='OK', headers=headers)
        await response.prepare(request)

        result = sqlite_db.get_restoration_matches(app_state.database_context, pushdata_hashes,
            json=True)
        for match in result:
            data = (json.dumps(match) + "\n").encode('utf-8')
            await response.write(data)

        await response.write(b"{}")
    return response


async def get_transaction(request: web.Request) -> web.Response:
    app_state: ApplicationState = request.app['app_state']
    accept_type = request.headers.get("Accept", "*/*")
    if accept_type == "*/*":
        accept_type = "application/json"

    tx_id = request.match_info['txid']
    if not tx_id:
        return web.Response(status=400, reason="no txid provided")

    try:
        tx_hash = hex_str_to_hash(tx_id)
    except ValueError:
        return web.Response(status=400, reason="invalid txid")

    rawtx = sqlite_db.get_transaction(app_state.database_context, tx_hash)
    if rawtx is None:
        return web.Response(status=404)

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
    accept_type = request.headers.get("Accept", "*/*")
    if accept_type == "*/*":
        accept_type = "application/json"

    txid = request.match_info['txid']
    if not txid:
        return web.Response(status=400, reason="no txid submitted")

    try:
        tx_hash = hex_str_to_hash(txid)
    except ValueError:
        return web.Response(status=400, reason="invalid txid")

    block_hash = sqlite_db.get_block_hash_for_tx(app_state.database_context, tx_hash)
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
                target = electrumsv_node.call_any("getblockheader", blockhash,
                    False).json()['result']
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
    accept_type = request.headers.get("Accept", "*/*")
    if accept_type == "*/*":
        accept_type = "application/json"
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
    existing_rows = sqlite_db.get_spent_outpoints(app_state.database_context, client_outpoints)

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
    accept_type = request.headers.get("Accept", "*/*")
    if accept_type == "*/*":
        accept_type = "application/json"

    content_type = request.headers.get("Content-Type")
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
        raise web.HTTPServiceUnavailable(reason="error finding synchronizer")

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
        raise web.HTTPServiceUnavailable(reason="error finding synchronizer")

    if len(client_outpoints) == 1 and client_outpoints[0] == ZEROED_OUTPOINT:
        synchronizer.clear_output_spend_notifications()
    else:
        synchronizer.unregister_output_spend_notifications(client_outpoints)
    return web.Response()


async def indexer_post_transaction_filter(request: web.Request) -> web.Response:
    """
    Optional endpoint if running an indexer.

    Used by the client to register pushdata hashes for new/updated transactions so that they can
    know about new occurrences of their pushdatas. This should be safe for consecutive calls
    even for the same pushdata, as the database unique constraint should raise an integrity
    error if there is an ongoing registration.
    """
    # TODO(1.4.0) Payment. This should be monetised with a free quota.
    app_state: ApplicationState = request.app['app_state']

    synchronizer = app_state.blockchain_state_monitor_thread
    if synchronizer is None:
        raise web.HTTPServiceUnavailable(reason="error finding synchronizer")

    accept_type = request.headers.get("Accept", "*/*")
    if accept_type == "*/*":
        accept_type = "application/json"
    if accept_type != "application/json":
        raise web.HTTPBadRequest(reason="only json response body supported")

    content_type = request.headers.get("Content-Type")
    if content_type not in ("application/octet-stream", "*/*"):
        raise web.HTTPBadRequest(reason="only binary request body supported")

    try:
        account_id_text = request.query.get("account_id", "")
        external_account_id = int(account_id_text)
    except (KeyError, ValueError):
        raise web.HTTPBadRequest(reason="`account_id` is not a number")

    try:
        date_created_text = request.query.get("date_created", "")
        date_created = int(date_created_text)
    except (KeyError, ValueError):
        raise web.HTTPBadRequest(reason="`date_created` is not a number")

    body = await request.content.read()
    if not body:
        raise web.HTTPBadRequest(reason="no body")

    if len(body) % tip_filter_entry_struct.size != 0:
        raise web.HTTPBadRequest(reason="binary request body malformed")

    # Currently we only allow registrations between five minutes and seven days.
    minimum_seconds = 5 * 60
    maximum_seconds = 7 * 24 * 60 * 60
    registration_entries = list[TipFilterRegistrationEntry]()
    for entry_index in range(len(body) // tip_filter_entry_struct.size):
        entry = TipFilterRegistrationEntry(*tip_filter_entry_struct.unpack_from(body,
            entry_index * tip_filter_entry_struct.size))
        if entry.duration_seconds < minimum_seconds:
            raise web.HTTPBadRequest(
                reason=f"An entry has a duration of {entry.duration_seconds} which is lower than "
                    f"the minimum value {minimum_seconds}")
        if entry.duration_seconds > maximum_seconds:
            raise web.HTTPBadRequest(
                reason=f"An entry has a duration of {entry.duration_seconds} which is higher than "
                    f"the maximum value {maximum_seconds}")
        registration_entries.append(entry)

    logger.debug("Adding tip filter entries to database %s", registration_entries)
    # It is required that the client knows what it is doing and this is enforced by disallowing
    # these registrations if any of the given pushdatas are already registered.
    account_id = sqlite_db.read_account_id_for_external_account_id(app_state.database_context,
        external_account_id)
    if not await app_state.database_context.run_in_thread_async(
            sqlite_db.create_indexer_filtering_registrations_pushdatas, account_id,
            date_created, registration_entries):
        raise web.HTTPBadRequest(reason="one or more hashes were already registered")

    logger.debug("Registering tip filter entries with synchroniser")
    # This can be registering hashes that other accounts have already registered, that is fine
    # as long as the registrations match the unregistrations.
    synchronizer.register_tip_filter_pushdatas(date_created, registration_entries)

    # Convert the timestamps to ISO 8601 time string. The posix timestamp is fine but we want to
    # default the public-facing API to JSON-developer level formats. We bundle it as an "object"
    # so that we can put additional things in there.
    date_created_text = datetime.utcfromtimestamp(date_created).isoformat()
    json_lump: TipFilterRegistrationResponse = {
        "dateCreated": date_created_text,
    }
    return web.json_response(data=json_lump)


async def indexer_post_transaction_filter_delete(request: web.Request) -> web.Response:
    """
    Optional endpoint if running an indexer.

    Used by the client to unregister pushdata hashes they are monitoring.
    """
    # TODO(1.4.0) Payment. This should be monetised with a free quota.
    app_state: ApplicationState = request.app['app_state']

    synchronizer = app_state.blockchain_state_monitor_thread
    if synchronizer is None:
        raise web.HTTPServiceUnavailable(reason="error finding synchronizer")

    accept_type = request.headers.get("Accept", "*/*")
    if accept_type == "*/*":
        accept_type = "application/octet-stream"
    if accept_type != "application/octet-stream":
        raise web.HTTPBadRequest(reason="only binary response body supported")

    content_type = request.headers.get("Content-Type")
    if content_type != 'application/octet-stream':
        raise web.HTTPBadRequest(reason="only binary request body supported")

    try:
        account_id_text = request.query.get("account_id", "")
        external_account_id = int(account_id_text)
    except (KeyError, ValueError):
        raise web.HTTPBadRequest(reason="`account_id` is not a number")

    body = await request.content.read()
    if not body:
        raise web.HTTPBadRequest(reason="no body")
    if len(body) % 32 != 0:
        raise web.HTTPBadRequest(reason="binary request body malformed")

    pushdata_hashes: set[bytes] = set()
    for pushdata_index in range(len(body) // 32):
        pushdata_hashes.add(body[pushdata_index:pushdata_index+32])
    if not len(pushdata_hashes):
        raise web.HTTPBadRequest(reason="no pushdata hashes provided")

    pushdata_hash_list = list(pushdata_hashes)

    # This is required to update all the given pushdata filtering registration from finalised
    # (and not being deleted by any other concurrent task) to finalised and being deleted. If
    # any of the registrations are not in this state, it is assumed that the client application
    # is broken and mismanaging it's own state.
    account_id = sqlite_db.read_account_id_for_external_account_id(app_state.database_context,
        external_account_id)
    try:
        await app_state.database_context.run_in_thread_async(
            sqlite_db.update_indexer_filtering_registrations_pushdatas_flags,
            account_id, pushdata_hash_list,
            update_flags=IndexerPushdataRegistrationFlag.DELETING,
            filter_flags=IndexerPushdataRegistrationFlag.FINALISED,
            filter_mask=IndexerPushdataRegistrationFlag.MASK_FINALISED_DELETING_CLEAR,
            require_all=True)
    except sqlite_db.DatabaseStateModifiedError:
        raise web.HTTPBadRequest(reason="some pushdata hashes are not registered")

    # It is essential that these registrations are in place either from a call during the current
    # run or read from the database on load, and that we are removing those registrations. If
    # entries not present get unregistered, this can corrupt the filter. If entries present get
    # unregistered but do not belong to this account, this will corrupt the filter.
    try:
        synchronizer.unregister_tip_filter_pushdatas(pushdata_hash_list)
    finally:
        await app_state.database_context.run_in_thread_async(
            sqlite_db.delete_indexer_filtering_registrations_pushdatas,
            account_id, pushdata_hash_list, IndexerPushdataRegistrationFlag.FINALISED,
            IndexerPushdataRegistrationFlag.FINALISED)

    return web.Response(status=200)

