from abc import abstractmethod
from typing import (
    cast,
    AsyncIterator,
    FrozenSet,
    Type,
)

from eth_typing import (
    Hash32,
)

from cancel_token import CancelToken, OperationCancelled

import ssz

from p2p import protocol
from p2p.peer import BasePeer, PeerSubscriber
from p2p.protocol import Command
from p2p.service import BaseService

from eth.exceptions import BlockNotFound

from eth2.beacon.chains.base import BeaconChain
from eth2.beacon.types.blocks import (
    BaseBeaconBlock,
    BeaconBlock,
)
from eth2.beacon.typing import (
    Slot,
)

from trinity.db.beacon.chain import BaseAsyncBeaconChainDB
from trinity.protocol.common.servers import BaseRequestServer
from trinity.protocol.common.peer import BasePeerPool
from trinity.protocol.bcc.commands import (
    BeaconBlocks,
    GetBeaconBlocks,
    GetBeaconBlocksMessage,
    NewBeaconBlock,
    NewBeaconBlockMessage,
)
from trinity.protocol.bcc.peer import (
    BCCPeer,
    BCCPeerPool,
)


class BCCRequestServer(BaseRequestServer):
    subscription_msg_types: FrozenSet[Type[Command]] = frozenset({
        GetBeaconBlocks,
    })

    def __init__(self,
                 db: BaseAsyncBeaconChainDB,
                 peer_pool: BCCPeerPool,
                 token: CancelToken = None) -> None:
        super().__init__(peer_pool, token)
        self.db = db

    async def _handle_msg(self, base_peer: BasePeer, cmd: Command,
                          msg: protocol._DecodedMsgType) -> None:
        peer = cast(BCCPeer, base_peer)
        self.logger.debug("cmd %s" % cmd)
        if isinstance(cmd, GetBeaconBlocks):
            await self._handle_get_beacon_blocks(peer, cast(GetBeaconBlocksMessage, msg))
        else:
            raise Exception(f"Invariant: Only subscribed to {self.subscription_msg_types}")

    async def _handle_get_beacon_blocks(self, peer: BCCPeer, msg: GetBeaconBlocksMessage) -> None:
        if not peer.is_operational:
            return

        request_id = msg["request_id"]
        max_blocks = msg["max_blocks"]
        block_slot_or_root = msg["block_slot_or_root"]

        try:
            if isinstance(block_slot_or_root, int):
                # TODO: pass accurate `block_class: Type[BaseBeaconBlock]` under
                # per BeaconStateMachine fork
                start_block = await self.db.coro_get_canonical_block_by_slot(
                    Slot(block_slot_or_root),
                    BeaconBlock,
                )
            elif isinstance(block_slot_or_root, bytes):
                # TODO: pass accurate `block_class: Type[BaseBeaconBlock]` under
                # per BeaconStateMachine fork
                start_block = await self.db.coro_get_block_by_root(
                    Hash32(block_slot_or_root),
                    BeaconBlock,
                )
            else:
                raise TypeError(
                    f"Invariant: unexpected type for 'block_slot_or_root': "
                    f"{type(block_slot_or_root)}"
                )
        except BlockNotFound:
            start_block = None

        if start_block is not None:
            self.logger.debug2(
                "%s requested %d blocks starting with %s",
                peer,
                max_blocks,
                start_block,
            )
            blocks = tuple([b async for b in self._get_blocks(start_block, max_blocks)])

        else:
            self.logger.debug2("%s requested unknown block %s", block_slot_or_root)
            blocks = ()

        self.logger.debug2("Replying to %s with %d blocks", peer, len(blocks))
        peer.sub_proto.send_blocks(blocks, request_id)

    async def _get_blocks(self,
                          start_block: BaseBeaconBlock,
                          max_blocks: int) -> AsyncIterator[BaseBeaconBlock]:
        if max_blocks < 0:
            raise Exception("Invariant: max blocks cannot be negative")

        if max_blocks == 0:
            return

        yield start_block

        try:
            # ensure only a connected chain is returned (breaks might occur if the start block is
            # not part of the canonical chain or if the canonical chain changes during execution)
            start = start_block.slot + 1
            end = start + max_blocks - 1
            parent = start_block
            for slot in range(start, end):
                # TODO: pass accurate `block_class: Type[BaseBeaconBlock]` under
                # per BeaconStateMachine fork
                block = await self.db.coro_get_canonical_block_by_slot(slot, BeaconBlock)
                if block.parent_root == parent.hash:
                    yield block
                else:
                    break
                parent = block
        except BlockNotFound:
            return


# FIXME: `BaseReceiveServer` is the same as `BaseRequestServer`.
# Since it's not settled that a `BaseReceiveServer` is needed and so
# in order not to pollute /trinity/protocol/common/servers.py,
# add the `BaseReceiveServer` here instead.
class BaseReceiveServer(BaseService, PeerSubscriber):
    """
    Monitor commands from peers, to identify inbound messages.
    Update local database according to different types of messages.
    """
    msg_queue_maxsize = 2000

    def __init__(
            self,
            peer_pool: BasePeerPool,
            token: CancelToken = None) -> None:
        super().__init__(token)
        self._peer_pool = peer_pool

    async def _run(self) -> None:
        self.run_daemon_task(self._handle_msg_loop())
        with self.subscribe(self._peer_pool):
            await self.cancellation()

    async def _handle_msg_loop(self) -> None:
        while self.is_operational:
            peer, cmd, msg = await self.wait(self.msg_queue.get())
            self.run_task(self._quiet_handle_msg(cast(BasePeer, peer), cmd, msg))

    async def _quiet_handle_msg(
            self,
            peer: BasePeer,
            cmd: protocol.Command,
            msg: protocol._DecodedMsgType) -> None:
        try:
            await self._handle_msg(peer, cmd, msg)
        except OperationCancelled:
            # Silently swallow OperationCancelled exceptions because otherwise they'll be caught
            # by the except below and treated as unexpected.
            pass
        except Exception:
            self.logger.exception("Unexpected error when processing msg from %s", peer)

    @abstractmethod
    async def _handle_msg(self, peer: BasePeer, cmd: Command, msg: protocol._DecodedMsgType) -> None:
        """
        Identify the command, and react appropriately.
        """
        pass


class BCCReceiveServer(BaseReceiveServer):
    subscription_msg_types: FrozenSet[Type[Command]] = frozenset({
        NewBeaconBlock,
    })

    def __init__(self,
                 chain: BeaconChain,
                 peer_pool: BCCPeerPool,
                 token: CancelToken = None) -> None:
        super().__init__(peer_pool, token)
        self.chain = chain

    async def _handle_msg(self, base_peer: BasePeer, cmd: Command,
                          msg: protocol._DecodedMsgType) -> None:
        peer = cast(BCCPeer, base_peer)
        self.logger.debug("cmd %s" % cmd)
        if isinstance(cmd, NewBeaconBlock):
            await self._handle_new_beacon_block(peer, cast(NewBeaconBlockMessage, msg))
        else:
            raise Exception(f"Invariant: Only subscribed to {self.subscription_msg_types}")

    async def _handle_new_beacon_block(self, peer: BCCPeer, msg: NewBeaconBlockMessage) -> None:
        if not peer.is_operational:
            return
        request_id = msg["request_id"]
        encoded_block = msg["encoded_block"]
        block = ssz.decode(encoded_block, BeaconBlock)
        self.logger.debug(f"!@# _handle_new_beacon_block: received request_id={request_id}, block={block}")  # noqa: E501
        # TODO: validate the block with db, and broadcast it if it's valid.

        # Persist the block and post state into the chain databsse.
        self.chain.import_block(block)
