from typing import (
    Any,
    NamedTuple,
    Union,
)

from rlp import sedes

from mypy_extensions import (
    TypedDict,
)

from eth_typing import (
    Hash32,
)

from p2p.protocol import (
    Command,
)

from trinity.rlp.sedes import (
    HashOrNumber,
)

from eth2.beacon.types.blocks import BeaconBlock
from eth2.beacon.types.attestations import Attestation


class StatusMessage(NamedTuple):
    protocol_version: int
    network_id: int
    genesis_hash: bytes
    best_hash: bytes


class Status(Command):
    _cmd_id = 0
    message_class = StatusMessage
    structure = [
        ('protocol_version', sedes.big_endian_int),
        ('network_id', sedes.big_endian_int),
        ('genesis_hash', sedes.binary),
        ('best_hash', sedes.binary),
    ]


GetBeaconBlocksMessage = TypedDict("GetBeaconBlocksMessage", {
    "block_slot_or_root": Union[int, Hash32],
    "max_blocks": int,
})


class GetBeaconBlocksMessage(NamedTuple):
    # TODO: Replace below Type Hint Appropriately
    block_slot_or_root: Any
    max_blocks: int


class GetBeaconBlocks(Command):
    _cmd_id = 1
    message_class = GetBeaconBlocksMessage
    structure = [
        ('block_slot_or_root', HashOrNumber()),
        ('max_blocks', sedes.big_endian_int),
    ]


class BeaconBlocks(Command):
    _cmd_id = 2
    structure = sedes.CountableList(BeaconBlock)


class AttestationRecords(Command):
    _cmd_id = 3
    structure = sedes.CountableList(Attestation)
