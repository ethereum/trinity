from hypothesis import (
    given,
)

import pytest

import rlp

from eth_utils import (
    decode_hex,
    int_to_big_endian,
    is_list_like,
)

from p2p.discv5.packets import (
    compute_encrypted_auth_response,
    AuthHeader,
    AuthHeaderPacket,
    AuthTagPacket,
    WhoAreYouPacket,
)
from p2p.discv5.enr import (
    ENR,
)
from p2p.discv5.messages import (
    default_message_type_registry,
    PingMessage,
)
from p2p.discv5.encryption import (
    aesgcm_decrypt,
)
from p2p.discv5.constants import (
    AUTH_RESPONSE_VERSION,
    AUTH_SCHEME_NAME,
    MAGIC_SIZE,
    ZERO_NONCE,
)
from p2p.discv5.identity_schemes import (
    V4IdentityScheme,
)

from tests.p2p.discv5.strategies import (
    key_st,
    nonce_st,
    public_key_st,
    tag_st,
    node_id_st,
    id_nonce_st,
    enr_seq_st,
    random_data_st,
)


@given(
    tag=tag_st,
    auth_tag=nonce_st,
    id_nonce=id_nonce_st,
    initiator_key=key_st,
    auth_response_key=key_st,
    ephemeral_public_key=public_key_st,
)
def test_auth_header_preparation(tag,
                                 auth_tag,
                                 id_nonce,
                                 initiator_key,
                                 auth_response_key,
                                 ephemeral_public_key):
    enr = ENR(
        sequence_number=1,
        signature=b"",
        kv_pairs={
            b"id": b"v4",
            b"secp256k1": b"\x02" * 33,
        }
    )
    message = PingMessage(
        request_id=5,
        enr_seq=enr.sequence_number,
    )
    id_nonce_signature = b"\x00" * 32

    packet = AuthHeaderPacket.prepare(
        tag=tag,
        auth_tag=auth_tag,
        id_nonce=id_nonce,
        message=message,
        initiator_key=initiator_key,
        id_nonce_signature=id_nonce_signature,
        auth_response_key=auth_response_key,
        enr=enr,
        ephemeral_public_key=ephemeral_public_key
    )

    assert packet.tag == tag
    assert packet.auth_header.auth_tag == auth_tag
    assert packet.auth_header.id_nonce == id_nonce
    assert packet.auth_header.auth_scheme_name == AUTH_SCHEME_NAME
    assert packet.auth_header.ephemeral_public_key == ephemeral_public_key

    decrypted_auth_response = aesgcm_decrypt(
        key=auth_response_key,
        nonce=ZERO_NONCE,
        cipher_text=packet.auth_header.encrypted_auth_response,
        authenticated_data=b"",
    )
    decoded_auth_response = rlp.decode(decrypted_auth_response)
    assert is_list_like(decoded_auth_response) and len(decoded_auth_response) == 3
    assert decoded_auth_response[0] == int_to_big_endian(AUTH_RESPONSE_VERSION)
    assert decoded_auth_response[1] == id_nonce_signature
    assert ENR.deserialize(decoded_auth_response[2]) == enr

    decrypted_message = aesgcm_decrypt(
        key=initiator_key,
        nonce=auth_tag,
        cipher_text=packet.encrypted_message,
        authenticated_data=tag,
    )
    assert decrypted_message[0] == message.message_type
    assert rlp.decode(decrypted_message[1:], PingMessage) == message


@given(
    tag=tag_st,
    auth_tag=nonce_st,
    random_data=random_data_st,
)
def test_random_packet_preparation(tag, auth_tag, random_data):
    packet = AuthTagPacket.prepare_random(
        tag=tag,
        auth_tag=auth_tag,
        random_data=random_data,
    )
    assert packet.tag == tag
    assert packet.auth_tag == auth_tag
    assert packet.encrypted_message == random_data


@given(
    tag=tag_st,
    auth_tag=nonce_st,
    id_nonce=id_nonce_st,
    initiator_key=key_st,
    auth_response_key=key_st,
    ephemeral_public_key=public_key_st,
)
def test_auth_header_preparation_without_enr(tag,
                                             auth_tag,
                                             id_nonce,
                                             initiator_key,
                                             auth_response_key,
                                             ephemeral_public_key):
    message = PingMessage(
        request_id=5,
        enr_seq=1,
    )
    id_nonce_signature = b"\x00" * 32

    packet = AuthHeaderPacket.prepare(
        tag=tag,
        auth_tag=auth_tag,
        id_nonce=id_nonce,
        message=message,
        initiator_key=initiator_key,
        id_nonce_signature=id_nonce_signature,
        auth_response_key=auth_response_key,
        enr=None,
        ephemeral_public_key=ephemeral_public_key
    )

    decrypted_auth_response = aesgcm_decrypt(
        key=auth_response_key,
        nonce=ZERO_NONCE,
        cipher_text=packet.auth_header.encrypted_auth_response,
        authenticated_data=b"",
    )
    decoded_auth_response = rlp.decode(decrypted_auth_response)
    assert is_list_like(decoded_auth_response) and len(decoded_auth_response) == 3
    assert decoded_auth_response[0] == int_to_big_endian(AUTH_RESPONSE_VERSION)
    assert decoded_auth_response[1] == id_nonce_signature
    assert decoded_auth_response[2] == []


@given(
    node_id=node_id_st,
    token=nonce_st,
    id_nonce=id_nonce_st,
    enr_seq=enr_seq_st,
)
def test_who_are_you_preparation(node_id, token, id_nonce, enr_seq):
    packet = WhoAreYouPacket.prepare(
        destination_node_id=node_id,
        token=token,
        id_nonce=id_nonce,
        enr_sequence_number=enr_seq,
    )
    assert packet.token == token
    assert packet.id_nonce == id_nonce
    assert packet.enr_sequence_number == enr_seq
    assert len(packet.magic) == MAGIC_SIZE


@given(
    tag=tag_st,
    auth_tag=nonce_st,
    key=key_st,
)
def test_auth_tag_packet_preparation(tag, auth_tag, key):
    message = PingMessage(
        request_id=5,
        enr_seq=3,
    )

    packet = AuthTagPacket.prepare(
        tag=tag,
        auth_tag=auth_tag,
        message=message,
        key=key,
    )
    assert packet.tag == tag
    assert packet.auth_tag == auth_tag
    decrypted_message = aesgcm_decrypt(
        key=key,
        nonce=auth_tag,
        cipher_text=packet.encrypted_message,
        authenticated_data=tag,
    )
    assert decrypted_message[0] == message.message_type
    assert rlp.decode(decrypted_message[1:], PingMessage) == message


@pytest.mark.parametrize(
    [
        "id_nonce",
        "secret_key",
        "enr",
        "auth_response_key",
        "ephemeral_public_key",
        "auth_cipher_text",
    ],
    [
        [
            decode_hex("0xe551b1c44264ab92bc0b3c9b26293e1ba4fed9128f3c3645301e8e119f179c65"),
            decode_hex("0x7e8107fe766b6d357205280acf65c24275129ca9e44c0fd00144ca50024a1ce7"),
            None,
            decode_hex("0x8c7caa563cebc5c06bb15fc1a2d426c3"),
            decode_hex(
                "0xb35608c01ee67edff2cffa424b219940a81cf2fb9b66068b1cf96862a17d353e22524fbdcdebc609"
                "f85cbd58ebe7a872b01e24a3829b97dd5875e8ffbc4eea81"
            ),
            decode_hex(
                "0x570fbf23885c674867ab00320294a41732891457969a0f14d11c995668858b2ad731aa7836888020"
                "e2ccc6e0e5776d0d4bc4439161798565a4159aa8620992fb51dcb275c4f755c8b8030c82918898f1ac"
                "387f606852"
            ),
        ],
    ],
)
def test_official_auth_response_encryption(secret_key,
                                           id_nonce,
                                           enr,
                                           auth_response_key,
                                           ephemeral_public_key,
                                           auth_cipher_text):
    id_nonce_signature = V4IdentityScheme.create_id_nonce_signature(
        id_nonce=id_nonce,
        private_key=secret_key,
        ephemeral_public_key=ephemeral_public_key,
    )
    assert compute_encrypted_auth_response(
        auth_response_key=auth_response_key,
        id_nonce_signature=id_nonce_signature,
        enr=enr,
    ) == auth_cipher_text


@pytest.mark.parametrize(
    [
        "auth_tag",
        "id_nonce",
        "ephemeral_public_key",
        "auth_cipher_text",
        "auth_header_rlp",
    ],
    [
        [
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
            decode_hex(
                "0xf8cc8c27b5af763c446acd2749fe8ea0e551b1c44264ab92bc0b3c9b26293e1ba4fed9128f3c3645"
                "301e8e119f179c658367636db840b35608c01ee67edff2cffa424b219940a81cf2fb9b66068b1cf968"
                "62a17d353e22524fbdcdebc609f85cbd58ebe7a872b01e24a3829b97dd5875e8ffbc4eea81b856570f"
                "bf23885c674867ab00320294a41732891457969a0f14d11c995668858b2ad731aa7836888020e2ccc6"
                "e0e5776d0d4bc4439161798565a4159aa8620992fb51dcb275c4f755c8b8030c82918898f1ac387f60"
                "6852"
            ),
        ],
    ]
)
def test_official_auth_header_encoding(auth_tag,
                                       id_nonce,
                                       ephemeral_public_key,
                                       auth_cipher_text,
                                       auth_header_rlp):
    header = AuthHeader(
        auth_tag=auth_tag,
        id_nonce=id_nonce,
        auth_scheme_name=AUTH_SCHEME_NAME,
        ephemeral_public_key=ephemeral_public_key,
        encrypted_auth_response=auth_cipher_text,
    )
    assert rlp.encode(header) == auth_header_rlp


@pytest.mark.parametrize(
    [
        "tag",
        "auth_tag",
        "id_nonce",
        "encoded_message",
        "local_private_key",
        "auth_response_key",
        "encryption_key",
        "ephemeral_public_key",
        "auth_message_rlp",
    ],
    [
        [
            decode_hex("0x93a7400fa0d6a694ebc24d5cf570f65d04215b6ac00757875e3f3a5f42107903"),
            decode_hex("0x27b5af763c446acd2749fe8e"),
            decode_hex("0xe551b1c44264ab92bc0b3c9b26293e1ba4fed9128f3c3645301e8e119f179c65"),
            decode_hex("0x01c20101"),
            decode_hex("0x7e8107fe766b6d357205280acf65c24275129ca9e44c0fd00144ca50024a1ce7"),
            decode_hex("0x8c7caa563cebc5c06bb15fc1a2d426c3"),
            decode_hex("0x9f2d77db7004bf8a1a85107ac686990b"),
            decode_hex(
                "0xb35608c01ee67edff2cffa424b219940a81cf2fb9b66068b1cf96862a17d353e22524fbdcdebc609"
                "f85cbd58ebe7a872b01e24a3829b97dd5875e8ffbc4eea81"
            ),
            decode_hex(
                "0x93a7400fa0d6a694ebc24d5cf570f65d04215b6ac00757875e3f3a5f42107903f8cc8c27b5af763c"
                "446acd2749fe8ea0e551b1c44264ab92bc0b3c9b26293e1ba4fed9128f3c3645301e8e119f179c6583"
                "67636db840b35608c01ee67edff2cffa424b219940a81cf2fb9b66068b1cf96862a17d353e22524fbd"
                "cdebc609f85cbd58ebe7a872b01e24a3829b97dd5875e8ffbc4eea81b856570fbf23885c674867ab00"
                "320294a41732891457969a0f14d11c995668858b2ad731aa7836888020e2ccc6e0e5776d0d4bc44391"
                "61798565a4159aa8620992fb51dcb275c4f755c8b8030c82918898f1ac387f606852a5d12a2d94b8cc"
                "b3ba55558229867dc13bfa3648"
            )
        ],
    ],
)
def test_official_auth_header_packet_preparation(tag,
                                                 auth_tag,
                                                 id_nonce,
                                                 encoded_message,
                                                 local_private_key,
                                                 auth_response_key,
                                                 encryption_key,
                                                 ephemeral_public_key,
                                                 auth_message_rlp):
    message_type_id = encoded_message[0]
    message_type = default_message_type_registry[message_type_id]
    message = rlp.decode(encoded_message[1:], message_type)
    assert message.to_bytes() == encoded_message

    id_nonce_signature = V4IdentityScheme.create_id_nonce_signature(
        id_nonce=id_nonce,
        ephemeral_public_key=ephemeral_public_key,
        private_key=local_private_key,
    )

    packet = AuthHeaderPacket.prepare(
        tag=tag,
        auth_tag=auth_tag,
        id_nonce=id_nonce,
        message=message,
        initiator_key=encryption_key,
        id_nonce_signature=id_nonce_signature,
        auth_response_key=auth_response_key,
        enr=None,
        ephemeral_public_key=ephemeral_public_key,
    )
    packet_wire_bytes = packet.to_wire_bytes()
    assert packet_wire_bytes == auth_message_rlp
