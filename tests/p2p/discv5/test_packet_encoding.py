import pytest

from hypothesis import (
    given,
    strategies as st,
)

from eth_utils import (
    decode_hex,
    ValidationError,
)

import rlp

from p2p.discv5.packets import (
    AuthTagPacket,
    AuthHeader,
    AuthHeaderPacket,
    WhoAreYouPacket,
    decode_message_packet,
    decode_packet,
    decode_who_are_you_packet,
)
from p2p.discv5.constants import (
    NONCE_SIZE,
    ID_NONCE_SIZE,
    TAG_SIZE,
    AUTH_SCHEME_NAME,
    MAGIC_SIZE,
)

from p2p.tools.factories import (
    AuthHeaderFactory,
    AuthHeaderPacketFactory,
    AuthTagPacketFactory,
    WhoAreYouPacketFactory,
)

from p2p.constants import DISCOVERY_MAX_PACKET_SIZE

from tests.p2p.discv5.strategies import (
    enr_seq_st,
    id_nonce_st,
    magic_st,
    nonce_st,
    tag_st,
)

# arbitrary as we're not working with a particular identity scheme
public_key_st = st.binary(min_size=33, max_size=33)


@given(
    tag=tag_st,
    auth_tag=nonce_st,
    encrypted_message_size=st.integers(
        min_value=0,
        # account for RLP prefix of auth tag
        max_value=DISCOVERY_MAX_PACKET_SIZE - (1 + TAG_SIZE) - NONCE_SIZE,
    ),
)
def test_auth_tag_packet_encoding_decoding(tag, auth_tag, encrypted_message_size):
    encrypted_message = b"\x00" * encrypted_message_size
    original_packet = AuthTagPacketFactory(
        tag=tag,
        auth_tag=auth_tag,
        encrypted_message=encrypted_message,
    )
    encoded_packet = original_packet.to_wire_bytes()
    decoded_packet = decode_message_packet(encoded_packet)
    assert isinstance(decoded_packet, AuthTagPacket)
    assert decoded_packet == original_packet


def test_oversize_auth_tag_packet_encoding():
    packet = AuthTagPacketFactory(
        encrypted_message=b"\x00" * (DISCOVERY_MAX_PACKET_SIZE - (1 + TAG_SIZE) - NONCE_SIZE + 1),
    )
    with pytest.raises(ValidationError):
        packet.to_wire_bytes()


@given(
    tag=tag_st,
    auth_tag=nonce_st,
    id_nonce=id_nonce_st,
    ephemeral_public_key=public_key_st,
    encrypted_auth_response=st.binary(min_size=16, max_size=32),
    encrypted_message_size=st.integers(
        min_value=0,
        # account for various RLP prefixes in auth header and assume max length for all entries
        max_value=DISCOVERY_MAX_PACKET_SIZE - TAG_SIZE - sum((
            2,  # rlp list prefix
            1 + NONCE_SIZE,  # tag
            1 + len(AUTH_SCHEME_NAME),  # auth scheme name
            1 + ID_NONCE_SIZE,  # id nonce
            1 + 33,  # public_key
            1 + 32,  # encrypted auth response
        ))
    ),
)
def test_auth_header_packet_encoding_decoding(tag,
                                              auth_tag,
                                              id_nonce,
                                              ephemeral_public_key,
                                              encrypted_auth_response,
                                              encrypted_message_size):
    auth_header = AuthHeaderFactory(
        auth_tag=auth_tag,
        id_nonce=id_nonce,
        ephemeral_public_key=ephemeral_public_key,
        encrypted_auth_response=encrypted_auth_response,
    )
    encrypted_message = b"\x00" * encrypted_message_size
    original_packet = AuthHeaderPacketFactory(
        tag=tag,
        auth_header=auth_header,
        encrypted_message=encrypted_message,
    )
    encoded_packet = original_packet.to_wire_bytes()
    decoded_packet = decode_message_packet(encoded_packet)
    assert isinstance(decoded_packet, AuthHeaderPacket)
    assert decoded_packet == original_packet


def test_oversize_auth_header_packet_encoding():
    auth_header = AuthHeaderFactory(encrypted_auth_response=32)
    header_size = len(rlp.encode(auth_header))
    encrypted_message_size = DISCOVERY_MAX_PACKET_SIZE - TAG_SIZE - header_size + 1
    encrypted_message = b"\x00" * encrypted_message_size
    packet = AuthHeaderPacketFactory(
        auth_header=auth_header,
        encrypted_message=encrypted_message,
    )
    with pytest.raises(ValidationError):
        packet.to_wire_bytes()


@pytest.mark.parametrize("encoded_packet", (
    b"",  # empty
    b"\x00" * TAG_SIZE,  # no auth section
    b"\x00" * 500,  # invalid RLP auth section
    # auth header with too few elements
    b"\x00" * TAG_SIZE + rlp.encode((
        b"\x00" * NONCE_SIZE,
        b"\x00" * ID_NONCE_SIZE,
        AUTH_SCHEME_NAME,
        b"",
    )),
    # auth header with too many elements
    b"\x00" * TAG_SIZE + rlp.encode((
        b"\x00" * NONCE_SIZE,
        b"\x00" * ID_NONCE_SIZE,
        AUTH_SCHEME_NAME,
        b"",
        b"",
        b"",
    )),
    # auth header with invalid tag
    b"\x00" * TAG_SIZE + rlp.encode((
        b"\x00" * (NONCE_SIZE - 1),
        b"\x00" * ID_NONCE_SIZE,
        AUTH_SCHEME_NAME,
        b"",
        b"",
    )),
    # auth header with invalid id nonce
    b"\x00" * TAG_SIZE + rlp.encode((
        b"\x00" * (NONCE_SIZE - 1),
        b"\x00" * (ID_NONCE_SIZE - 1),
        AUTH_SCHEME_NAME,
        b"",
        b"",
    )),
    # auth header with wrong auth scheme
    b"\x00" * TAG_SIZE + rlp.encode((
        b"\x00" * NONCE_SIZE,
        b"\x00" * ID_NONCE_SIZE,
        b"no-gcm",
        b"",
        b"",
    )),
    # auth header with tag being a list
    b"\x00" * TAG_SIZE + rlp.encode((
        [b"\x00"] * NONCE_SIZE,
        b"\x00" * ID_NONCE_SIZE,
        b"gcm",
        b"",
        b"",
    )),
    # auth header with id nonce being a list
    b"\x00" * TAG_SIZE + rlp.encode((
        b"\x00" * NONCE_SIZE,
        [b"\x00"] * ID_NONCE_SIZE,
        b"gcm",
        b"",
        b"",
    )),
    # auth header with public key being a list
    b"\x00" * TAG_SIZE + rlp.encode((
        b"\x00" * NONCE_SIZE,
        b"\x00" * ID_NONCE_SIZE,
        b"gcm",
        [b""],
        b"",
    )),
    # auth header with auth response being a list
    b"\x00" * TAG_SIZE + rlp.encode((
        b"\x00" * NONCE_SIZE,
        b"\x00" * ID_NONCE_SIZE,
        b"gcm",
        b"",
        [b""],
    )),
    # auth header with oversized message
    b"\x00" * TAG_SIZE + rlp.encode((
        b"\x00" * NONCE_SIZE,
        b"\x00" * ID_NONCE_SIZE,
        b"gcm",
        b"",
        b"",
    )) + b"\x00" * 2000,

    # auth tag with invalid size
    b"\x00" * TAG_SIZE + rlp.encode(b"\x00" * (NONCE_SIZE - 1)),
    # auth tag with oversized message
    b"\x00" * TAG_SIZE + rlp.encode(b"\x00" * NONCE_SIZE) + b"\x00" * 2000,
))
def test_invalid_message_packet_decoding(encoded_packet):
    with pytest.raises(ValidationError):
        decode_message_packet(encoded_packet)


@given(
    magic=magic_st,
    token=nonce_st,
    id_nonce=id_nonce_st,
    enr_seq=enr_seq_st,
)
def test_who_are_you_encoding_decoding(magic, token, id_nonce, enr_seq):
    original_packet = WhoAreYouPacket(
        magic=magic,
        token=token,
        id_nonce=id_nonce,
        enr_sequence_number=enr_seq,
    )
    encoded_packet = original_packet.to_wire_bytes()
    decoded_packet = decode_who_are_you_packet(encoded_packet)
    assert decoded_packet == original_packet


@pytest.mark.parametrize("encoded_packet", (
    b"",  # empty
    b"\x00" * MAGIC_SIZE,  # no payload
    b"\x00" * 500,  # invalid RLP payload
    b"\x00" * MAGIC_SIZE + rlp.encode(b"payload"),  # payload is not a list
    # payload too short
    b"\x00" * MAGIC_SIZE + rlp.encode((b"\x00" * NONCE_SIZE, b"")),
    # payload too long
    b"\x00" * MAGIC_SIZE + rlp.encode((b"\x00" * NONCE_SIZE, b"", b"", b"")),
    b"\x00" * (MAGIC_SIZE - 1) + rlp.encode((b"\x00" * NONCE_SIZE, b"", 0)),  # too short
    b"\x00" * MAGIC_SIZE + rlp.encode((b"\x00" * 11, b"", 0)),  # invalid nonce
    # too long
    b"\x00" * MAGIC_SIZE + rlp.encode((b"\x00" * NONCE_SIZE, b"\x00" * 2000, 0)),
))
def test_invalid_who_are_you_decoding(encoded_packet):
    with pytest.raises(ValidationError):
        decode_who_are_you_packet(encoded_packet)


def test_invalid_who_are_you_encoding():
    packet = WhoAreYouPacket(
        magic=b"\x00" * MAGIC_SIZE,
        token=b"\x00" * NONCE_SIZE,
        id_nonce=b"\x00" * 2000,
        enr_sequence_number=0,
    )
    with pytest.raises(ValidationError):
        packet.to_wire_bytes()


@pytest.mark.parametrize("packet", (
    WhoAreYouPacketFactory(),
    AuthTagPacketFactory(),
    AuthHeaderPacketFactory(),
))
def test_packet_decoding(packet):
    encoded_packet = packet.to_wire_bytes()
    decoded_packet = decode_packet(encoded_packet)
    assert decoded_packet == packet


# official test vectors from the ethereum/devp2p repository
@pytest.mark.parametrize(
    ["prep_function", "params", "encoded"],
    [
        [
            AuthTagPacket,
            {
                "tag": decode_hex(
                    "0x0101010101010101010101010101010101010101010101010101010101010101"
                ),
                "auth_tag": decode_hex("0x020202020202020202020202"),
                "encrypted_message": decode_hex(
                    "0x0404040404040404040404040404040404040404040404040404040404040404040404040404"
                    "040404040404"
                ),
            },
            decode_hex(
                "0x01010101010101010101010101010101010101010101010101010101010101018c02020202020202"
                "0202020202040404040404040404040404040404040404040404040404040404040404040404040404"
                "0404040404040404"
            ),
        ],
        [
            WhoAreYouPacket,
            {
                "magic": decode_hex(
                    "0x0101010101010101010101010101010101010101010101010101010101010101"
                ),
                "token": decode_hex(
                    "0x020202020202020202020202"
                ),
                "id_nonce": decode_hex(
                    "0x0303030303030303030303030303030303030303030303030303030303030303"
                ),
                "enr_sequence_number": 0x01,
            },
            decode_hex(
                "0101010101010101010101010101010101010101010101010101010101010101ef8c02020202020202"
                "0202020202a0030303030303030303030303030303030303030303030303030303030303030301"
            ),
        ],
    ]
)
def test_official_basic(prep_function, params, encoded):
    packet = prep_function(**params)
    assert packet.to_wire_bytes() == encoded
    assert decode_packet(encoded) == packet


@pytest.mark.parametrize(
    [
        "tag",
        "auth_tag",
        "id_nonce",
        "ephemeral_pubkey",
        "auth_response_cipher_text",
        "message_cipher_text",
        "encoded_packet",
    ],
    [
        [
            decode_hex("0x93a7400fa0d6a694ebc24d5cf570f65d04215b6ac00757875e3f3a5f42107903"),
            decode_hex("0x27b5af763c446acd2749fe8e"),
            decode_hex("0xe551b1c44264ab92bc0b3c9b26293e1ba4fed9128f3c3645301e8e119f179c65"),
            decode_hex(
                "0xb35608c01ee67edff2cffa424b219940a81cf2fb9b66068b1cf96862a17d353e22524fbdcdebc609"
                "f85cbd58ebe7a872b01e24a3829b97dd5875e8ffbc4eea81"
            ),
            decode_hex(
                "0x570fbf23885c674867ab00320294a41732891457969a0f14d11c995668858b2ad731aa7836888020"
                "e2ccc6e0e5776d0d4bc4439161798565a4159aa8620992fb51dcb275c4f755c8b8030c82918898f1ac"
                "387f606852"
            ),
            decode_hex("0xa5d12a2d94b8ccb3ba55558229867dc13bfa3648"),
            decode_hex(
                "0x93a7400fa0d6a694ebc24d5cf570f65d04215b6ac00757875e3f3a5f42107903f8cc8c27b5af763c"
                "446acd2749fe8ea0e551b1c44264ab92bc0b3c9b26293e1ba4fed9128f3c3645301e8e119f179c6583"
                "67636db840b35608c01ee67edff2cffa424b219940a81cf2fb9b66068b1cf96862a17d353e22524fbd"
                "cdebc609f85cbd58ebe7a872b01e24a3829b97dd5875e8ffbc4eea81b856570fbf23885c674867ab00"
                "320294a41732891457969a0f14d11c995668858b2ad731aa7836888020e2ccc6e0e5776d0d4bc44391"
                "61798565a4159aa8620992fb51dcb275c4f755c8b8030c82918898f1ac387f606852a5d12a2d94b8cc"
                "b3ba55558229867dc13bfa3648"
            ),
        ]
    ]
)
def test_official_message(tag,
                          auth_tag,
                          id_nonce,
                          ephemeral_pubkey,
                          auth_response_cipher_text,
                          message_cipher_text,
                          encoded_packet):

    header = AuthHeader(
        auth_tag,
        id_nonce,
        AUTH_SCHEME_NAME,
        ephemeral_pubkey,
        auth_response_cipher_text,
    )
    packet = AuthHeaderPacket(tag, header, message_cipher_text)
    assert packet.to_wire_bytes() == encoded_packet
    assert decode_packet(encoded_packet) == packet
