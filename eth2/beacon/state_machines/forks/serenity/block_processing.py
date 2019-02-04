from eth_utils.toolz import (
    first,
)

from eth2._utils.tuple import update_tuple_item
from eth2._utils.numeric import (
    bitwise_xor,
)
from eth2.beacon._utils.hash import hash_eth2

from eth2.beacon.configs import BeaconConfig
from eth2.beacon.types.states import BeaconState
from eth2.beacon.types.blocks import BaseBeaconBlock
from eth2.beacon.types.eth1_data_vote import Eth1DataVote

from eth2.beacon.state_machines.forks.serenity.block_validation import (
    validate_randao_reveal,
)

from eth2.beacon.helpers import (
    get_beacon_proposer_index,
    get_current_epoch,
    get_randao_mix,
)


def process_eth1_data(state: BeaconState,
                      block: BaseBeaconBlock,
                      config: BeaconConfig) -> BeaconState:
    try:
        vote_index, original_vote = first(
            (index, eth1_data_vote)
            for index, eth1_data_vote in enumerate(state.eth1_data_votes)
            if block.eth1_data == eth1_data_vote.eth1_data
        )
    except StopIteration:
        new_vote = Eth1DataVote(
            eth1_data=block.eth1_data,
            vote_count=1,
        )
        state = state.copy(
            eth1_data_votes=state.eth1_data_votes + (new_vote,)
        )
    else:
        updated_vote = original_vote.copy(
            vote_count=original_vote.vote_count + 1
        )
        state = state.copy(
            eth1_data_votes=update_tuple_item(state.eth1_data_votes, vote_index, updated_vote)
        )

    return state


from eth2.beacon.typing import (
    SlotNumber,
)


def process_randao(state: BeaconState,
                   block: BaseBeaconBlock,
                   config: BeaconConfig) -> BeaconState:
    proposer_index = get_beacon_proposer_index(
        state=state,
        slot=state.slot,
        epoch_length=config.EPOCH_LENGTH,
        target_committee_size=config.TARGET_COMMITTEE_SIZE,
        shard_count=config.SHARD_COUNT,
    )
    proposer = state.validator_registry[proposer_index]

    epoch = get_current_epoch(state, config.EPOCH_LENGTH)

    validate_randao_reveal(
        randao_reveal=block.randao_reveal,
        proposer_pubkey=proposer.pubkey,
        epoch=epoch,
        fork=state.fork,
    )

    randao_mix_index = epoch % config.LATEST_RANDAO_MIXES_LENGTH
    # FIXME: remove this once get_randao_mix is updated to accept epochs instead of slots
    slot = SlotNumber(epoch)
    new_randao_mix = bitwise_xor(
        get_randao_mix(state, slot, config.LATEST_RANDAO_MIXES_LENGTH),
        hash_eth2(block.randao_reveal),
    )

    return state.copy(
        latest_randao_mixes=update_tuple_item(
            state.latest_randao_mixes,
            randao_mix_index,
            new_randao_mix,
        ),
    )
