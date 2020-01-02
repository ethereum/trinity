import asyncio
from typing import (
    Tuple,
)

from eth_utils.toolz import (
    partition_all,
)
from eth2.beacon.exceptions import (
    NoCommitteeAssignment,
)
from eth2.beacon.helpers import compute_start_slot_at_epoch
from eth2.beacon.state_machines.forks.skeleton_lake.config import (
    MINIMAL_SERENITY_CONFIG,
)
from eth2.beacon.tools.factories import (
    BeaconChainFactory,
)
from eth2.beacon.tools.builder.proposer import (
    is_proposer,
)
from eth2.beacon.tools.builder.validator import mk_key_pair_from_seed_index, mk_keymap_of_size
from eth2.beacon.tools.misc.ssz_vector import (
    override_lengths,
)
from trinity.components.eth2.beacon.validator import (
    Validator,
)
from trinity.protocol.bcc_libp2p.node import PeerPool


override_lengths(MINIMAL_SERENITY_CONFIG)


class FakeNode:
    def __init__(self):
        self.list_beacon_block = []

    async def broadcast_beacon_block(self, block):
        self.list_beacon_block.append(block)

    async def broadcast_attestation(self, attestation):
        pass

    async def broadcast_attestation_to_subnet(self, attestation, subnet_id):
        pass

    async def broadcast_beacon_aggregate_and_proof(self, aggregate_and_proof):
        pass

    handshaked_peers = PeerPool()


def create_validator(
    event_bus,
    monkeypatch,
    indices,
    p2p_node,
    num_validators=None,
    base_db=None,
) -> Validator:
    if num_validators is not None:
        chain = BeaconChainFactory(num_validators=num_validators, base_db=base_db)
    else:
        chain = BeaconChainFactory(base_db=base_db)

    validator_privkeys = {
        index: mk_key_pair_from_seed_index(index)[1]
        for index in indices
    }

    # Mock attestation pool
    unaggregated_attestation_pool = set()
    aggregated_attestation_pool = set()

    def get_ready_attestations_fn(slot, is_aggregated):
        return tuple(unaggregated_attestation_pool)

    def get_aggregatable_attestations_fn(slot, committee_index):
        return tuple(unaggregated_attestation_pool)

    def import_attestation_fn(attestation, is_aggregated):
        if is_aggregated:
            aggregated_attestation_pool.add(attestation)
        else:
            unaggregated_attestation_pool.add(attestation)

    return Validator(
        chain=chain,
        p2p_node=p2p_node,
        validator_privkeys=validator_privkeys,
        get_ready_attestations_fn=get_ready_attestations_fn,
        get_aggregatable_attestations_fn=get_aggregatable_attestations_fn,
        import_attestation_fn=import_attestation_fn,
        event_bus=event_bus,
    )

    # Make requesting eth1 vote and deposit a stub
    async def _get_eth1_vote(slot, state, state_machine):
        return None
    monkeypatch.setattr(v, '_get_eth1_vote', _get_eth1_vote)

    async def _get_deposit_data(state, state_machine, eth1_vote):
        return None
    monkeypatch.setattr(v, '_get_deposit_data', _get_deposit_data)


async def get_validator(event_loop, event_bus, monkeypatch, handshaked_peers, indices, num_validators=None) -> Validator:
    libp2p_node = FakeNode()
    v = create_validator(
        event_bus,
        monkeypatch,
        indices,
        libp2p_node,
        num_validators=num_validators,
    )
    asyncio.ensure_future(v.run(), loop=event_loop)
    await v.events.started.wait()
    # yield to `validator._run`
    await asyncio.sleep(0)
    return v


async def get_linked_validators(
    event_loop, event_bus, num_validators, monkeypatch
) -> Tuple[Validator, Validator]:
    keymap = mk_keymap_of_size(num_validators)
    all_indices = tuple(
        index for index in range(len(keymap))
    )
    global_peer_count = 2
    alice_indices, bob_indices = partition_all(
        len(all_indices) // global_peer_count,
        all_indices
    )
    alice = await get_validator(event_loop, event_bus, monkeypatch, alice_indices)
    bob = await get_validator(event_loop, event_bus, monkeypatch, bob_indices)
    return alice, bob


def get_slot_with_validator_selected(candidate_indices, state, config):
    epoch = state.current_epoch(config.SLOTS_PER_EPOCH)
    epoch_start_slot = compute_start_slot_at_epoch(epoch, config.SLOTS_PER_EPOCH)

    for index in candidate_indices:
        try:
            for slot in range(epoch_start_slot, epoch_start_slot + config.SLOTS_PER_EPOCH):
                state = state.set("slot", slot)
                if is_proposer(state, index, config):
                    return slot, index
        except NoCommitteeAssignment:
            continue
    raise Exception(
        "Check the parameters of the genesis state; the above code should return"
        " some proposer if the set of ``candidate_indices`` is big enough."
    )
