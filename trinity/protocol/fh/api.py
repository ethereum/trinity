from typing import cast, Any, Dict, Tuple

from cached_property import cached_property

from eth_typing import BlockNumber, Hash32
from eth_utils import humanize_hash
from lru import LRU

from p2p.abc import ConnectionAPI
from p2p.exchange import ExchangeAPI, ExchangeLogic
from p2p.logic import Application, CommandHandler
from p2p.qualifiers import HasProtocol
from p2p.typing import Payload

from .commands import NewBlock
from .constants import MAX_WITNESS_HISTORY_PER_PEER
from .exchanges import (
    GetBlockBodiesExchange,
    GetBlockHeadersExchange,
    GetNodeDataExchange,
    GetReceiptsExchange,
)
from .handshaker import FirehoseHandshakeReceipt
from .proto import FirehoseProtocol


class RecentWitnesses(CommandHandler):
    command_type = NewBlockWitnessHashes

    _recent_witness_hashes: LRU

    def __init__(self) -> None:
        self._recent_witness_hashes = LRU(MAX_WITNESS_HISTORY_PER_PEER)

    async def handle(self, connection: ConnectionAPI, msg: Payload) -> None:
        msg = cast(Dict[str, Any], msg)
        header_hash = msg['block_hash']
        witness_hashes = msg['node_hashes']

        self._recent_witness_hashes[header_hash] = witness_hashes
        self.logger.warning(
            "Got %d witness hashes on block %s from %s",
            len(witness_hashes),
            humanize_hash(header_hash),
            connection,
        )

    def has_witness(self, header_hash: Hash32) -> bool:
        return header_hash in self._recent_witness_hashes

    def pop_node_hashes(self, header_hash: Hash32) -> Tuple[Hash32]:
        return self._recent_witness_hashes.pop(header_hash)


class FirehoseAPI(Application):
    name = 'fh'  # TODO: Is this always the same value as in fh/proto?
    qualifier = HasProtocol(FirehoseProtocol)

    witnesses: RecentWitnesses

    def __init__(self) -> None:
        self.witnesses = RecentWitnesses()
        self.add_child_behavior(self.witnesses.as_behavior())

    @cached_property
    def exchanges(self) -> Tuple[ExchangeAPI[Any, Any, Any], ...]:
        return ()

    @cached_property
    def protocol(self) -> FirehoseProtocol:
        return self.connection.get_protocol_by_type(FirehoseProtocol)

    def send_new_block_witness_hashes(self, header_hash: Hash32, node_hashes: Sequence[Hash32]) -> None:
        # Trying the new API that might not exist yet
        payload = NewBlockWitnessHashesPayload(header_hash, node_hashes)
        self.protocol.send(NewBlockWitnessHashes(payload))
