from enum import IntFlag

ZMQ_NODE_PORT = 28332
ZMQ_TOPIC_HASH_BLOCK = b"hashblock"
ZMQ_TOPIC_HASH_TX = b"hashtx"
NULL_HASH = "0000000000000000000000000000000000000000000000000000000000000000"
GENESIS_HASH = "0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206"

MAX_UINT32 = 2**32 - 1

# Converted to a 1 byte char flag for pushdata matches
OUTPUT_MATCH = 1 << 0
INPUT_MATCH = 1 << 1

SERVER_HOST = "127.0.0.1"
SERVER_PORT = 49241

REFERENCE_SERVER_SCHEME = "http"
REFERENCE_SERVER_HOST = "127.0.0.1"
REFERENCE_SERVER_PORT = 47126

# TODO(1.4.0) Technical debt. This is unused. Use it or remove it.
COMMON_FILTER_LOOKAHEAD_BATCH_COUNT = 20



class AccountFlag(IntFlag):
    NONE                                    = 0


class OutboundDataFlag(IntFlag):
    NONE                                    = 0

    TIP_FILTER_NOTIFICATIONS                = 1 << 0

    DISPATCHED_SUCCESSFULLY                 = 1 << 20
