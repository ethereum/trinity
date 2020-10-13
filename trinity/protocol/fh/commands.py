from typing import (
    NamedTuple,
    Tuple,
)

from eth_typing import Hash32

from rlp import sedes

from p2p.commands import BaseCommand, RLPCodec

from trinity.protocol.eth.forkid import ForkID
from trinity.rlp.sedes import hash_sedes

STATUS_STRUCTURE = sedes.List((
    sedes.big_endian_int,
    sedes.big_endian_int,
    ForkID,
))


class StatusPayload(NamedTuple):
    version: int
    network_id: int
    fork_id: ForkID


class Status(BaseCommand[StatusPayload]):
    protocol_command_id = 0
    serialization_codec = RLPCodec(
        sedes=STATUS_STRUCTURE,
        process_inbound_payload_fn=lambda args: StatusPayload(*args),
    )


class NewBlockWitnessHashesPayload(NamedTuple):
    block_hash: Hash32
    node_hashes: Tuple[Hash32, ...]


class NewBlockWitnessHashes(BaseCommand[NewBlockWitnessHashesPayload]):
    protocol_command_id = 1
    serialization_codec = RLPCodec(
        sedes=sedes.List((hash_sedes, sedes.CountableList(hash_sedes))),
        process_inbound_payload_fn=lambda args: NewBlockWitnessHashesPayload(*args),
    )
