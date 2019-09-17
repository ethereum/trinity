import pytest

from eth2.beacon.tools.factories import BeaconChainFactory

from trinity.protocol.bcc_libp2p.topic_validators import (
    get_ancestor_state,
)
from trinity.protocol.bcc_libp2p.configs import (
    PUBSUB_TOPIC_BEACON_ATTESTATION,
    PUBSUB_TOPIC_BEACON_BLOCK,
)

from tests.libp2p.bcc.test_receive_server import get_blocks


@pytest.mark.parametrize("num_nodes", (1,))
@pytest.mark.asyncio
async def test_setup_topic_validators(nodes):
    node = nodes[0]
    topic_1 = PUBSUB_TOPIC_BEACON_BLOCK
    topic_2 = PUBSUB_TOPIC_BEACON_ATTESTATION
    assert topic_1 in node.pubsub.topic_validators
    assert topic_2 in node.pubsub.topic_validators


@pytest.mark.parametrize(
    "num_blocks, block_slots, target_block_slot, latest_finalized_slot, expected_block_slot",
    (
        (5, range(5), 1, 0, 0),
        (5, range(0, 10, 2), 5, 4, 4),
        (5, range(0, 10, 2), 4, 4, None),
    ),
)
def test_get_ancestor_state(
    monkeypatch,
    num_blocks,
    block_slots,
    target_block_slot,
    latest_finalized_slot,
    expected_block_slot,
):
    chain = BeaconChainFactory()
    blocks = list(get_blocks(chain, num_blocks=num_blocks))
    for i, slot in enumerate(block_slots):
        blocks[i] = blocks[i].copy(slot=slot)
        if i > 0:
            blocks[i] = blocks[i].copy(parent_root=blocks[i - 1].signing_root)
    db = {block.signing_root: block for block in blocks}

    def get_block_by_root(root):
        return db[root]

    def get_state_by_slot(slot):
        if slot == expected_block_slot:
            return True
        return False

    monkeypatch.setattr(chain, "get_block_by_root", get_block_by_root)
    monkeypatch.setattr(chain, "get_state_by_slot", get_state_by_slot)

    if expected_block_slot is not None:
        assert get_ancestor_state(
            chain, blocks[-1], latest_finalized_slot, target_block_slot
        )
    else:
        with pytest.raises(ValueError):
            get_ancestor_state(
                chain, blocks[-1], latest_finalized_slot, target_block_slot
            )
