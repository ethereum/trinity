from p2p.constants import (
    DISCOVERY_MAX_PACKET_SIZE,
)
from p2p.typing import Nonce

NONCE_SIZE = 12  # size of an AESGCM nonce
TAG_SIZE = 32  # size of the tag packet prefix
MAGIC_SIZE = 32  # size of the magic hash in the who are you packet
ID_NONCE_SIZE = 32  # size of the id nonce in who are you and auth tag packets
RANDOM_ENCRYPTED_DATA_SIZE = 12  # size of random data we send to initiate a handshake
# safe upper bound on the size of the ENR list in a nodes message
NODES_MESSAGE_PAYLOAD_SIZE = DISCOVERY_MAX_PACKET_SIZE - 200

ZERO_NONCE = Nonce(b"\x00" * NONCE_SIZE)  # nonce used for the auth header packet
AUTH_RESPONSE_VERSION = 5  # version number used in auth response
AUTH_SCHEME_NAME = b"gcm"  # the name of the only supported authentication scheme

TOPIC_HASH_SIZE = 32  # size of a topic hash

WHO_ARE_YOU_MAGIC_SUFFIX = b"WHOAREYOU"

MAX_REQUEST_ID = 2**32 - 1  # highest request id used for outgoing requests
MAX_REQUEST_ID_ATTEMPTS = 100  # number of attempts we take to guess a available request id

REQUEST_RESPONSE_TIMEOUT = 0.5  # timeout for waiting for response after request was sent
# timeout for waiting for node messages in response to find node requests
FIND_NODE_RESPONSE_TIMEOUT = 1.0
HANDSHAKE_TIMEOUT = 1  # timeout for performing a handshake
ROUTING_TABLE_PING_INTERVAL = 30  # interval of outgoing pings sent to maintain the routing table
ROUTING_TABLE_LOOKUP_INTERVAL = 60  # intervals between lookups
LOOKUP_RETRY_THRESHOLD = 5  # minimum number of ENRs desired in responses to FindNode requests
LOOKUP_PARALLELIZATION_FACTOR = 3  # number of parallel lookup requests (aka alpha)

MAX_NODES_MESSAGE_TOTAL = 8  # max allowed total value for nodes messages
