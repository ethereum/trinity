import math

import pytest

import trio

from p2p.trio_service import (
    Manager,
)

from p2p.discv5.enr import (
    UnsignedENR,
)
from p2p.discv5.identity_schemes import (
    V4IdentityScheme,
)
from p2p.discv5.handshake import (
    HandshakeInitiator,
    HandshakeRecipient,
)


#
# Fixtures
#
@pytest.fixture
def initiator_key_pair():
    return V4IdentityScheme.create_random_key_pair()


@pytest.fixture
def initiator_private_key(initiator_key_pair):
    private_key, _ = initiator_key_pair
    return private_key


@pytest.fixture
def initiator_public_key(initiator_key_pair):
    _, public_key = initiator_key_pair
    return public_key


@pytest.fixture
def recipient_key_pair():
    return V4IdentityScheme.create_random_key_pair()


@pytest.fixture
def recipient_private_key(recipient_key_pair):
    private_key, _ = recipient_key_pair
    return private_key


@pytest.fixture
def recipient_public_key(recipient_key_pair):
    _, public_key = recipient_key_pair
    return public_key


@pytest.fixture
def initiator_enr(initiator_private_key, initiator_public_key):
    return UnsignedENR(
        sequence_number=3,
        kv_pairs={
            b"id": b"v4",
            b"secp256k1": initiator_public_key,
        }
    ).to_signed_enr(initiator_private_key)


@pytest.fixture
def recipient_enr(recipient_private_key, recipient_public_key):
    return UnsignedENR(
        sequence_number=3,
        kv_pairs={
            b"id": b"v4",
            b"secp256k1": recipient_public_key,
        }
    ).to_signed_enr(recipient_private_key)


@pytest.fixture
def initiator_endpoint():
    return Endpoint(
        ip_address=b"initiator",
        port=123456,
    )


@pytest.fixture
def recipient_endpoint():
    return Endpoint(
        ip_address=b"recipient",
        port=123456,
    )


@pytest.fixture
def run_handshake_test(initiator_private_key,
                       recipient_private_key,
                       initiator_enr,
                       recipient_enr,
                       initiator_endpoint,
                       recipient_endpoint):

    async def run_handshake_test(additional_initiator_kwargs=None,
                                 additional_recipient_kwargs=None):
        # packet channels
        initiator_outgoing_send, initiator_outgoing_receive = trio.open_memory_channel(0)
        recipient_incoming_send, recipient_incoming_receive = trio.open_memory_channel(0)
        recipient_outgoing_send, recipient_outgoing_receive = trio.open_memory_channel(0)
        initiator_incoming_send, initiator_incoming_receive = trio.open_memory_channel(0)

        async def bridge_packet_channels(outgoing_receive, incoming_send):
            async with outgoing_receive:
                async for outgoing_packet in outgoing_receive:
                    if outgoing_packet.receiver == recipient_endpoint:
                        sender = initiator_endpoint
                    elif outgoing_packet.receiver == initiator_endpoint:
                        sender = recipient_endpoint
                    else:
                        assert False, "Unknown receiver endpoint"

                    incoming_packet = IncomingPacket(
                        packet=outgoing_packet.packet,
                        sender=sender,
                    )
                    await incoming_send.send(incoming_packet)

        # use unbounded channels so that we can process the results at the end without risking to
        # block the handshake services
        initiator_session_keys_channel_pair = trio.open_memory_channel(math.inf)
        recipient_session_keys_channel_pair = trio.open_memory_channel(math.inf)
        recipient_enr_update_channel_pair = trio.open_memory_channel(math.inf)
        recipient_message_channel_pair = trio.open_memory_channel(math.inf)

        initial_message = OutgoingMessage(
            message=PingMessage(request_id=5, enr_seq=recipient_enr.sequence_number),
            node_id=recipient_enr.node_id,
            receiver=recipient_endpoint,
        )
        handshake_initiator = HandshakeInitiator(**{
            **{
                "peer_enr": recipient_enr,
                "local_enr": initiator_enr,
                "local_private_key": initiator_private_key,
                "initial_message": initial_message,
                "incoming_packet_receive_channel": initiator_incoming_receive,
                "outgoing_packet_send_channel": initiator_outgoing_send,
                "session_keys_send_channel": initiator_session_keys_channel_pair[0],
            },
            **(additional_initiator_kwargs or {}),
        })

        async def run_handshake_recipient():
            initiating_packet = await recipient_incoming_receive.receive()
            handshake_recipient = HandshakeRecipient(**{
                **{
                    "peer_node_id": initiator_enr.node_id,
                    "peer_enr": initiator_enr,
                    "local_enr": recipient_enr,
                    "local_private_key": recipient_private_key,
                    "initiating_packet": initiating_packet,
                    "incoming_packet_receive_channel": recipient_incoming_receive,
                    "outgoing_packet_send_channel": recipient_outgoing_send,
                    "session_keys_send_channel": recipient_session_keys_channel_pair[0],
                    "enr_update_send_channel": recipient_enr_update_channel_pair[0],
                    "incoming_message_send_channel": recipient_message_channel_pair[0],
                },
                **(additional_recipient_kwargs or {}),
            })
            await Manager.run(handshake_recipient)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(
                bridge_packet_channels,
                initiator_outgoing_receive,
                recipient_incoming_send,
            )
            nursery.start_soon(
                bridge_packet_channels,
                recipient_outgoing_receive,
                initiator_incoming_send,
            )

            async with trio.open_nursery() as handshake_nursery:
                handshake_nursery.start_soon(Manager.run, handshake_initiator)
                handshake_nursery.start_soon(run_handshake_recipient)

            nursery.cancel_scope.cancel()


def assert_empty(receive_channel):
    with pytest.raises(trio.WouldBlock):
        receive_channel.receive_nowait()


#
# Tests
#
async def test_session_keys_are_equal(run_handshake_test):
    initiator_session_keys_channel_pair = trio.open_memory_channel(math.inf)
    recipient_session_keys_channel_pair = trio.open_memory_channel(math.inf)

    with trio.fail_after(0.5):
        run_handshake_test(
            additional_initiator_kwargs={
                "session_keys_send_channel": initiator_session_keys_channel_pair[0],
            },
            additional_recipient_kwargs={
                "session_keys_send_channel": recipient_session_keys_channel_pair[0],
            },
        )

    initiator_session_keys = initiator_session_keys_channel_pair[1].receive_nowait()
    recipient_session_keys = recipient_session_keys_channel_pair[1].receive_nowait()
    assert initiator_session_keys == recipient_session_keys
    assert_empty(initiator_session_keys_channel_pair[1])
    assert_empty(recipient_session_keys_channel_pair[1])


async def test_enr_update(run_handshake_test, initiator_enr, initiator_private_key):
    enr_channel_pair = trio.open_memory_channel(math.inf)

    # no ENR is sent if recipient knows the most up to date one
    with trio.fail_after(0.5):
        run_handshake_test(
            additional_recipient_kwargs={
                "peer_enr": initiator_enr,
                "enr_update_send_channel": enr_channel_pair[0],
            },
        )
    assert_empty(enr_channel_pair[1])

    # ENR is sent if recipient knows no ENR at all
    with trio.fail_after(0.5):
        run_handshake_test(
            additional_recipient_kwargs={
                "peer_enr": None,
                "enr_update_send_channel": enr_channel_pair[0],
            },
        )
    enr = enr_channel_pair[1].receive_nowait()
    assert enr == initiator_enr
    assert_empty(enr_channel_pair[1])

    # ENR is sent if recipient only knows outdated ENR
    outdated_enr = UnsignedENR(
        sequence_number=initiator_enr.sequence_number - 1,
        kv_pairs=dict(initiator_enr),
    ).to_signed_enr(initiator_private_key)
    with trio.fail_after(0.5):
        run_handshake_test(
            additional_recipient_kwargs={
                "peer_enr": outdated_enr,
                "enr_update_send_channel": enr_channel_pair[0],
            },
        )
    enr = enr_channel_pair[1].receive_nowait()
    assert enr == initiator_enr
    assert_empty(enr_channel_pair[1])


async def test_message_is_transmitted(run_handshake_test,
                                      initiator_endpoint,
                                      recipient_endpoint,
                                      recipient_enr):
    message_channel_pair = trio.open_memory_channel(math.inf)

    initial_message = OutgoingMessage(
        message=PingMessage(request_id=0, enr_seq=recipient_enr.sequence_number),
        node_id=recipient_enr.node_id,
        receiver=recipient_endpoint,
    )
    with trio.fail_after(0.5):
        run_handshake_test(
            additional_initiator_kwargs={
                "initial_message": initial_message,
            },
            additional_recipient_kwargs={
                "incoming_message_send_channel": message_channel_pair[0],
            },
        )

    received_message = message_channel_pair[1].receive_nowait()
    assert received_message.sender == initiator_endpoint
    assert received_message.node_id == initiator_enr.node_id
    assert received_message.message == initial_message.message
    assert_empty(message_channel_pair[1])
