from p2p.discv5.typing import (
    Nonce,
)


AES128_KEY_SIZE = 16  # size of an AES218 key
NONCE_SIZE = 12  # size of an AESGCM nonce
TAG_SIZE = 32  # size of the tag packet prefix
MAGIC_SIZE = 32  # size of the magic hash in the who are you packet
ID_NONCE_SIZE = 32  # size of the id nonce in who are you and auth tag packets
RANDOM_ENCRYPTED_DATA_SIZE = 12  # size of random data we send to initiate a handshake

ZERO_NONCE = Nonce(b"\x00" * NONCE_SIZE)  # nonce used for the auth header packet
AUTH_RESPONSE_VERSION = 5  # version number used in auth response
AUTH_SCHEME_NAME = b"gcm"  # the name of the only supported authentication scheme

TOPIC_HASH_SIZE = 32  # size of a topic hash
IP_V4_SIZE = 4  # size of an IPv4 address
IP_V6_SIZE = 16  # size of an IPv6 address

ENR_REPR_PREFIX = "enr:"  # prefix used when printing an ENR
MAX_ENR_SIZE = 300  # maximum allowed size of an ENR
IP_V4_ADDRESS_ENR_KEY = b"ip"
UDP_PORT_ENR_KEY = b"udp"
TCP_PORT_ENR_KEY = b"tcp"

WHO_ARE_YOU_MAGIC_SUFFIX = b"WHOAREYOU"
HKDF_INFO = b"discovery v5 key agreement"

MAX_REQUEST_ID = 2**32 - 1  # highest request id used for outgoing requests
MAX_REQUEST_ID_ATTEMPTS = 100  # number of attempts we take to guess a available request id

REQUEST_RESPONSE_TIMEOUT = 0.5  # timeout for waiting for response after request was sent
HANDSHAKE_TIMEOUT = 1  # timeout for performing a handshake
ROUTING_TABLE_PING_INTERVAL = 5  # interval of outgoing pings sent to maintain the routing table

NUM_ROUTING_TABLE_BUCKETS = 256  # number of buckets in the routing table
