import asyncio
from typing import Awaitable, Callable, List, Tuple
from trinity._utils.logging import get_logger

from cached_property import cached_property

from eth_typing import Hash32
from eth_utils import (
    humanize_hash,
)
from lru import LRU

from p2p.abc import ConnectionAPI
from p2p.logic import Application, CommandHandler
from p2p.qualifiers import HasProtocol

from .commands import (
    NewBlockWitnessHashes,
    NewBlockWitnessHashesPayload,
)
from .constants import MAX_WITNESS_HISTORY_PER_PEER
from .proto import FirehoseProtocol


class RecentWitnesses(CommandHandler[NewBlockWitnessHashes]):
    command_type = NewBlockWitnessHashes

    _recent_witness_hashes: LRU

    def __init__(self) -> None:
        self.logger = get_logger('trinity.protocol.fh.api.RecentWitnesses')
        self._recent_witness_hashes = LRU(MAX_WITNESS_HISTORY_PER_PEER)
        self._subscribers: List[Callable[[NewBlockWitnessHashesPayload], Awaitable[None]]] = []

    async def handle(self, connection: ConnectionAPI, cmd: NewBlockWitnessHashes) -> None:
        header_hash, witness_hashes = cmd.payload
        self.logger.info(
            "Peer behavior is handling %d witness hashes on block %s from %s",
            len(witness_hashes),
            humanize_hash(header_hash),
            connection,
        )

        self._recent_witness_hashes[header_hash] = witness_hashes

        if self._subscribers:
            await asyncio.gather(*(
                handler(cmd.payload) for handler in self._subscribers
            ))

    def subscribe(
            self, callback: Callable[[NewBlockWitnessHashesPayload], Awaitable[None]]) -> None:
        self._subscribers.append(callback)

    def has_witness(self, header_hash: Hash32) -> bool:
        return header_hash in self._recent_witness_hashes

    def pop_node_hashes(self, header_hash: Hash32) -> Tuple[Hash32, ...]:
        hashes = self._recent_witness_hashes[header_hash]
        del self._recent_witness_hashes[header_hash]
        return hashes

    def get_node_hashes(self, header_hash: Hash32) -> Tuple[Hash32, ...]:
        return self._recent_witness_hashes[header_hash]


class FirehoseAPI(Application):
    name = 'fh'
    qualifier = HasProtocol(FirehoseProtocol)

    def __init__(self) -> None:
        self.logger = get_logger('trinity.protocol.fh.api.FirehoseAPI')
        self.witnesses = RecentWitnesses()
        self.add_child_behavior(self.witnesses.as_behavior())

    @cached_property
    def protocol(self) -> FirehoseProtocol:
        return self.connection.get_protocol_by_type(FirehoseProtocol)

    def get_extra_stats(self) -> Tuple[str, ...]:
        # TODO: Can't use BasePerformanceTracker in RecentWitnesses for performance stats as
        # that's not an exchange. Need another tracker here.
        return tuple()

    def send_new_block_witness_hashes(
            self, header_hash: Hash32, node_hashes: Tuple[Hash32, ...]) -> None:
        """
        This will skip sending if the peer already sent the witness hashes to us.
        """
        if self.witnesses.has_witness(header_hash):
            self.logger.info(
                "SKIP Sending %d hashes of witness to: %s",
                len(node_hashes),
                self.connection,
            )
            # remove witness from history, as cleanup
            received_witness_hashes = self.witnesses.pop_node_hashes(header_hash)
            if set(node_hashes) != set(received_witness_hashes):
                self.logger.warning(
                    "Remote node generated a different witness than we did locally! " +
                    "Sizes: remote: %d, local: %d",
                    len(set(received_witness_hashes)),
                    len(set(node_hashes)),
                )
        else:
            self.logger.info(
                "Sending %d hashes of witness to: %s", len(node_hashes), self.connection)
            payload = NewBlockWitnessHashesPayload(header_hash, node_hashes)
            self.protocol.send(NewBlockWitnessHashes(payload))
