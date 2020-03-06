import inspect

from eth_utils import (
    decode_hex,
)

import pytest

import rlp
from rlp.sedes import (
    big_endian_int,
)

from p2p.discv5 import messages
from p2p.enr import ENR
from p2p.discv5.messages import (
    default_message_type_registry,
    BaseMessage,
    PingMessage,
    PongMessage,
    NodesMessage,
    FindNodeMessage,
)


def test_default_message_registry():
    message_data_classes = tuple(
        member for _, member in inspect.getmembers(messages)
        if inspect.isclass(member) and issubclass(member, BaseMessage) and member is not BaseMessage
    )
    assert len(default_message_type_registry) == len(message_data_classes)
    for message_data_class in message_data_classes:
        message_type = message_data_class.message_type
        assert message_type in default_message_type_registry
        assert default_message_type_registry[message_type] is message_data_class


def test_all_messages_have_request_id():
    for message_data_class in default_message_type_registry.values():
        first_field_name, first_field_sedes = message_data_class._meta.fields[0]
        assert first_field_name == "request_id"
        assert first_field_sedes == big_endian_int


def test_message_types_are_continuous():
    sorted_message_types = sorted(default_message_type_registry.keys())
    assert sorted_message_types == list(range(1, len(sorted_message_types) + 1))


def test_message_encoding():
    message = PingMessage(request_id=5, enr_seq=10)
    encoded_message = message.to_bytes()
    assert encoded_message[0] == message.message_type
    assert encoded_message[1:] == rlp.encode(message)


# Official test vectors from
# https://github.com/ethereum/devp2p/blob/master/discv5/discv5-wire-test-vectors.md
# TODO: Add official test vectors for topic messages once they are aligned with the current
# version of the spec
@pytest.mark.parametrize(["message_type", "params", "message_rlp"], [
    [
        PingMessage,
        {
            "request_id": 0x01,
            "enr_seq": 0x01,
        },
        decode_hex("0x01c20101"),
    ],
    [
        PongMessage,
        {
            "request_id": 0x01,
            "enr_seq": 0x01,
            "packet_ip": bytes([127, 0, 0, 1]),
            "packet_port": 5000,
        },
        decode_hex("0x02ca0101847f000001821388"),
    ],
    [
        FindNodeMessage,
        {
            "request_id": 0x01,
            "distance": 0x0100,
        },
        decode_hex("0x03c401820100"),
    ],
    [
        NodesMessage,
        {
            "request_id": 0x01,
            "total": 0x01,
            "enrs": [],
        },
        decode_hex("0x04c30101c0"),
    ],
    [
        NodesMessage,
        {
            "request_id": 0x01,
            "total": 0x01,
            "enrs": [
                ENR.from_repr(
                    "enr:-HW4QBzimRxkmT18hMKaAL3IcZF1UcfTMPyi3Q1pxwZZbcZVRI8DC5infUAB_UauARLOJtYTxa"
                    "agKoGmIjzQxO2qUygBgmlkgnY0iXNlY3AyNTZrMaEDymNMrg1JrLQB2KTGtv6MVbcNEVv0AHacwUAP"
                    "MljNMTg"
                ),
                ENR.from_repr(
                    "enr:-HW4QNfxw543Ypf4HXKXdYxkyzfcxcO-6p9X986WldfVpnVTQX1xlTnWrktEWUbeTZnmgOuAY_"
                    "KUhbVV1Ft98WoYUBMBgmlkgnY0iXNlY3AyNTZrMaEDDiy3QkHAxPyOgWbxp5oF1bDdlYE6dLCUUp8x"
                    "fVw50jU"
                ),
            ]
        },
        decode_hex(
            "0x04f8f20101f8eef875b8401ce2991c64993d7c84c29a00bdc871917551c7d330fca2dd0d69c706596dc6"
            "55448f030b98a77d4001fd46ae0112ce26d613c5a6a02a81a6223cd0c4edaa532801826964827634897365"
            "63703235366b31a103ca634cae0d49acb401d8a4c6b6fe8c55b70d115bf400769cc1400f3258cd3138f875"
            "b840d7f1c39e376297f81d7297758c64cb37dcc5c3beea9f57f7ce9695d7d5a67553417d719539d6ae4b44"
            "5946de4d99e680eb8063f29485b555d45b7df16a1850130182696482763489736563703235366b31a1030e"
            "2cb74241c0c4fc8e8166f1a79a05d5b0dd95813a74b094529f317d5c39d235"
        ),
    ],
])
def test_official_message_encodings(message_type, params, message_rlp):
    message = message_type(**params)
    assert message.to_bytes() == message_rlp
    assert message_rlp[0] == message_type.message_type
    assert rlp.decode(message_rlp[1:], message_type) == message
