from eth2.beacon.constants import GENESIS_PARENT_ROOT
from eth2.beacon.types.attestations import Attestation
from eth2.beacon.types.blocks import BeaconBlock, BeaconBlockBody, SignedBeaconBlock
from eth2.beacon.typing import FromBlockParams


def test_defaults(sample_beacon_block_params):
    block = BeaconBlock.create(**sample_beacon_block_params)
    assert block.slot == sample_beacon_block_params["slot"]
    assert block.is_genesis


def test_update_attestations(sample_attestation_params, sample_beacon_block_params):
    block = BeaconBlock.create(**sample_beacon_block_params)
    attestations = block.body.attestations
    attestations = list(attestations)
    attestations.append(Attestation.create(**sample_attestation_params))
    body2 = block.body.set("attestations", attestations)
    block2 = block.set("body", body2)
    assert len(block2.body.attestations) == 1


def test_block_body_empty(sample_attestation_params):
    block_body = BeaconBlockBody.create()
    assert tuple(block_body.proposer_slashings) == ()
    assert tuple(block_body.attester_slashings) == ()
    assert tuple(block_body.attestations) == ()
    assert tuple(block_body.deposits) == ()
    assert tuple(block_body.voluntary_exits) == ()

    assert block_body.is_empty

    block_body = block_body.set(
        "attestations", (Attestation.create(**sample_attestation_params),)
    )
    assert not block_body.is_empty


def test_block_root_and_block_header_root_equivalence():
    block = BeaconBlock.create()
    header = block.header

    assert block.hash_tree_root == header.hash_tree_root


def test_block_is_not_genesis():
    genesis_block = SignedBeaconBlock.create().transform(
        ("message", "parent_root"), GENESIS_PARENT_ROOT
    )
    another_block = SignedBeaconBlock.from_parent(genesis_block, FromBlockParams())
    assert genesis_block.is_genesis
    assert not another_block.is_genesis
