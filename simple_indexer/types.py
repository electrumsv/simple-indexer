import struct
import typing
from typing import TypedDict, Optional, NamedTuple


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
