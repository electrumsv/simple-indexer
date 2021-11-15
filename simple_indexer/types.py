import enum
import struct
import typing
from typing import TypedDict, Optional, NamedTuple, Dict

import bitcoinx


class RestorationFilterRequest(typing.TypedDict):
    filterKeys: typing.List[str]


class RestorationFilterJSONResponse(TypedDict):
    flags: int
    pushDataHashHex: str
    transactionId: str
    index: int
    spendTransactionId: Optional[str]
    spendInputIndex: int
    blockHeight: int


class RestorationFilterResult(NamedTuple):
    flags: bytes
    push_data_hash: bytes
    transaction_hash: bytes
    spend_transaction_hash: bytes
    transaction_output_index: int
    spend_input_index: int
    block_height: int


RESULT_UNPACK_FORMAT = ">c32s32s32sIII"
FILTER_RESPONSE_SIZE = 1 + 32 + 32 + 32 + 4 + 4 + 4
assert struct.calcsize(RESULT_UNPACK_FORMAT) == FILTER_RESPONSE_SIZE

filter_response_struct = struct.Struct(RESULT_UNPACK_FORMAT)


def le_int_to_char(le_int):
    return struct.pack('<I', le_int)[0:1]


class TxOrId(enum.IntEnum):
    TRANSACTION_ID = 0
    FULL_TRANSACTION = 1

class TargetType(enum.IntEnum):
    HASH = 0
    HEADER = 1 << 1
    MERKLE_ROOT = 1 << 2


class ProofType(enum.IntEnum):
    MERKLE_BRANCH = 0
    MERKLE_TREE = 1 << 3


class CompositeProof(enum.IntEnum):
    SINGLE_PROOF = 0
    COMPOSITE_PROOF = 1 << 4


def tsc_merkle_proof_json_to_binary(tsc_json: Dict, include_full_tx: bool, target_type: str) \
        -> bytearray:
    """{'index': 0, 'txOrId': txOrId, 'target': target, 'nodes': []}"""
    response = bytearray()

    flags = 0
    if include_full_tx:
        flags = flags & TxOrId.FULL_TRANSACTION

    if target_type == 'hash':
        flags = flags & TargetType.HASH
    elif target_type == 'header':
        flags = flags & TargetType.HEADER
    elif target_type == 'merkleroot':
        flags = flags & TargetType.MERKLE_ROOT
    else:
        flags = flags & TargetType.HASH

    flags = flags & ProofType.MERKLE_BRANCH  # ProofType.MERKLE_TREE not supported
    flags = flags & CompositeProof.SINGLE_PROOF  # CompositeProof.COMPOSITE_PROOF not supported

    response += le_int_to_char(flags)
    response += bitcoinx.pack_varint(tsc_json['index'])

    if include_full_tx:
        txLength = len(tsc_json['txOrId'])
        response += bitcoinx.pack_varint(txLength)
        response += bytes.fromhex(tsc_json['txOrId'])
    else:
        response += bitcoinx.hex_str_to_hash(tsc_json['txOrId'])

    if target_type in {'hash', 'merkleroot'}:
        response += bitcoinx.hex_str_to_hash(tsc_json['target'])
    else:  # header
        response += bytes.fromhex(tsc_json['target'])

    nodeCount = bitcoinx.pack_varint(len(tsc_json['nodes']))
    response += nodeCount
    for node in tsc_json['nodes']:
        if node == "*":
            duplicate_type_node = b'\x01'
            response += duplicate_type_node
        else:
            hash_type_node = b"\x00"
            response += hash_type_node
            response += bitcoinx.hex_str_to_hash(node)
    return response
