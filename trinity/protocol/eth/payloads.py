from typing import NamedTuple, Tuple

from eth_typing import BlockNumber, Hash32

from eth.abc import BlockHeaderAPI

from trinity.protocol.eth.forkid import ForkID
from trinity.rlp.sedes import UninterpretedTransaction


class StatusV63Payload(NamedTuple):
    version: int
    network_id: int
    total_difficulty: int
    head_hash: Hash32
    genesis_hash: Hash32


class StatusPayload(NamedTuple):
    version: int
    network_id: int
    total_difficulty: int
    head_hash: Hash32
    genesis_hash: Hash32
    fork_id: ForkID


class NewBlockHash(NamedTuple):
    hash: Hash32
    number: BlockNumber


class BlockFields(NamedTuple):
    header: BlockHeaderAPI
    transactions: Tuple[UninterpretedTransaction, ...]
    uncles: Tuple[BlockHeaderAPI, ...]


class NewBlockPayload(NamedTuple):
    block: BlockFields
    total_difficulty: int
