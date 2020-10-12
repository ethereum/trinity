from typing import (
    NamedTuple,
    Tuple,
)

from eth_typing import Hash32

from rlp import sedes

from p2p.commands import BaseCommand, RLPCodec

from trinity.rlp.sedes import hash_sedes


class GetBlockWitnessHashesPayload(NamedTuple):
    request_id: int
    block_hash: Hash32


class GetBlockWitnessHashes(BaseCommand[GetBlockWitnessHashesPayload]):
    protocol_command_id = 1
    serialization_codec = RLPCodec(
        sedes=sedes.List((sedes.big_endian_int, hash_sedes)),
        process_inbound_payload_fn=lambda args: GetBlockWitnessHashesPayload(*args),
    )


class BlockWitnessHashesPayload(NamedTuple):
    request_id: int
    node_hashes: Tuple[Hash32, ...]


class BlockWitnessHashes(BaseCommand[BlockWitnessHashesPayload]):
    protocol_command_id = 2
    serialization_codec = RLPCodec(
        sedes=sedes.List((sedes.big_endian_int, sedes.CountableList(hash_sedes))),
        process_inbound_payload_fn=lambda args: BlockWitnessHashesPayload(*args),
    )
