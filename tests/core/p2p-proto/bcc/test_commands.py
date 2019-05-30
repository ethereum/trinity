import pytest

import ssz

from eth.constants import (
    ZERO_HASH32,
)

from eth2.beacon.types.attestations import Attestation
from eth2.beacon.types.attestation_data import AttestationData
from eth2.beacon.types.blocks import (
    BeaconBlock,
    BeaconBlockBody,
)
from eth2.beacon.types.crosslinks import Crosslink

from p2p.peer import (
    MsgBuffer,
)

from trinity.protocol.bcc.commands import (
    BeaconBlocks,
    GetBeaconBlocks,
    Attestations,
)

from .helpers import (
    get_directly_linked_peers,
    get_genesis_chain_db,
)

from eth2.beacon.constants import EMPTY_SIGNATURE
from eth2.beacon.state_machines.forks.serenity.configs import SERENITY_CONFIG


async def get_command_setup(request, event_loop):
    alice, bob = await get_directly_linked_peers(
        request,
        event_loop,
        alice_chain_db=await get_genesis_chain_db(),
        bob_chain_db=await get_genesis_chain_db(),
    )
    msg_buffer = MsgBuffer()
    bob.add_subscriber(msg_buffer)

    return alice, msg_buffer


@pytest.mark.asyncio
async def test_send_no_blocks(request, event_loop):
    alice, msg_buffer = await get_command_setup(request, event_loop)

    request_id = 5
    alice.sub_proto.send_blocks((), request_id=request_id)

    message = await msg_buffer.msg_queue.get()
    assert isinstance(message.command, BeaconBlocks)
    assert message.payload == {
        "request_id": request_id,
        "encoded_blocks": (),
    }


@pytest.mark.asyncio
async def test_send_single_block(request, event_loop):
    alice, msg_buffer = await get_command_setup(request, event_loop)

    request_id = 5
    block = BeaconBlock(
        slot=1,
        previous_block_root=ZERO_HASH32,
        state_root=ZERO_HASH32,
        signature=EMPTY_SIGNATURE,
        body=BeaconBlockBody.create_empty_body(),
    )
    alice.sub_proto.send_blocks((block,), request_id=request_id)

    message = await msg_buffer.msg_queue.get()
    assert isinstance(message.command, BeaconBlocks)
    assert message.payload == {
        "request_id": request_id,
        "encoded_blocks": (ssz.encode(block),),
    }


@pytest.mark.asyncio
async def test_send_multiple_blocks(request, event_loop):
    alice, msg_buffer = await get_command_setup(request, event_loop)

    request_id = 5
    blocks = tuple(
        BeaconBlock(
            slot=slot,
            previous_block_root=ZERO_HASH32,
            state_root=ZERO_HASH32,
            signature=EMPTY_SIGNATURE,
            body=BeaconBlockBody.create_empty_body(),
        )
        for slot in range(3)
    )
    alice.sub_proto.send_blocks(blocks, request_id=request_id)

    message = await msg_buffer.msg_queue.get()
    assert isinstance(message.command, BeaconBlocks)
    assert message.payload == {
        "request_id": request_id,
        "encoded_blocks": tuple(ssz.encode(block) for block in blocks),
    }


@pytest.mark.asyncio
async def test_send_get_blocks_by_slot(request, event_loop):
    alice, msg_buffer = await get_command_setup(request, event_loop)

    request_id = 5
    alice.sub_proto.send_get_blocks(123, 10, request_id=request_id)

    message = await msg_buffer.msg_queue.get()
    assert isinstance(message.command, GetBeaconBlocks)
    assert message.payload == {
        "request_id": request_id,
        "block_slot_or_root": 123,
        "max_blocks": 10,
    }


@pytest.mark.asyncio
async def test_send_get_blocks_by_hash(request, event_loop):
    alice, msg_buffer = await get_command_setup(request, event_loop)

    request_id = 5
    alice.sub_proto.send_get_blocks(b"\x33" * 32, 15, request_id=request_id)

    message = await msg_buffer.msg_queue.get()
    assert isinstance(message.command, GetBeaconBlocks)
    assert message.payload == {
        "request_id": request_id,
        "block_slot_or_root": b"\x33" * 32,
        "max_blocks": 15,
    }


@pytest.mark.asyncio
async def test_send_no_attestations(request, event_loop):
    alice, msg_buffer = await get_command_setup(request, event_loop)

    alice.sub_proto.send_attestation_records(())

    message = await msg_buffer.msg_queue.get()
    assert isinstance(message.command, Attestations)
    assert message.payload == {
        "encoded_attestations": (),
    }


@pytest.mark.asyncio
async def test_send_single_attestation(request, event_loop):
    alice, msg_buffer = await get_command_setup(request, event_loop)

    attestation = Attestation(
        aggregation_bitfield=b"\x00\x00\x00",
        data=AttestationData(
            slot=0,
            beacon_block_root=ZERO_HASH32,
            source_epoch=SERENITY_CONFIG.GENESIS_EPOCH,
            target_root=ZERO_HASH32,
            source_root=ZERO_HASH32,
            shard=1,
            previous_crosslink=Crosslink(SERENITY_CONFIG.GENESIS_EPOCH, ZERO_HASH32),
            crosslink_data_root=ZERO_HASH32,
        ),
        custody_bitfield=b"\x00\x00\x00",
    )

    alice.sub_proto.send_attestation_records((attestation,))

    message = await msg_buffer.msg_queue.get()
    assert isinstance(message.command, Attestations)
    assert message.payload["encoded_attestations"] == (ssz.encode(attestation),)


@pytest.mark.asyncio
async def test_send_multiple_attestations(request, event_loop):
    alice, msg_buffer = await get_command_setup(request, event_loop)

    attestations = tuple(
        Attestation(
            aggregation_bitfield=b"\x00\x00\x00",
            data=AttestationData(
                slot=0,
                beacon_block_root=ZERO_HASH32,
                source_epoch=SERENITY_CONFIG.GENESIS_EPOCH,
                target_root=ZERO_HASH32,
                source_root=ZERO_HASH32,
                shard=shard,
                previous_crosslink=Crosslink(SERENITY_CONFIG.GENESIS_EPOCH, ZERO_HASH32),
                crosslink_data_root=ZERO_HASH32,
            ),
            custody_bitfield=b"\x00\x00\x00",
        ) for shard in range(10)
    )

    alice.sub_proto.send_attestation_records(attestations)

    message = await msg_buffer.msg_queue.get()
    assert isinstance(message.command, Attestations)
    assert message.payload["encoded_attestations"] == tuple(
        ssz.encode(attestation) for attestation in attestations)
