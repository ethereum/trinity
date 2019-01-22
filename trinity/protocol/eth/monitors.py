from typing import (
    Any,
    AsyncIterator,
    cast,
    Dict,
    Tuple,
)

from cancel_token import CancelToken
from eth_utils import ValidationError

from eth.rlp.headers import (
    BlockHeader
)

from p2p.exceptions import PeerConnectionLost
from p2p.peer import BasePeer, PeerSubscriber
from p2p.service import BaseService

from trinity.protocol.common.monitors import BaseChainTipMonitor
from trinity.protocol.common.peer import BaseChainPeer, BaseChainPeerPool
from trinity.protocol.eth import commands


class ETHChainTipMonitor(BaseChainTipMonitor):
    subscription_msg_types = frozenset({commands.NewBlock})


class ETHVerifiedTipMonitor(BaseService, PeerSubscriber):
    """
    Monitor for potential changes to the tip of the chain: a new peer or a new block

    Fetches and verifies and yields the new highest header

    Subclass must specify :attr:`subscription_msg_types`
    """
    # This is a rather arbitrary value, but when the sync is operating normally we never see
    # the msg queue grow past a few hundred items, so this should be a reasonable limit for
    # now.
    msg_queue_maxsize = 2000
    subscription_msg_types = frozenset({commands.NewBlock})

    def __init__(
            self,
            peer_pool: BaseChainPeerPool,
            token: CancelToken = None) -> None:
        super().__init__(token)
        self._peer_pool = peer_pool
        self._subscriber_notice = self.get_event_loop().create_future()

    async def wait_tip_info(self) -> AsyncIterator[Tuple[BaseChainPeer, BlockHeader]]:
        """
        This iterator waits until there is potentially new tip information.
        New tip information means a new peer connected or a new block arrived.
        Then it yields the peer with the highest total difficulty.
        It continues indefinitely, until this service is cancelled.
        """
        if self.is_cancelled:
            raise ValidationError("%s is cancelled, new tip info is impossible", self)
        elif not self.is_running:
            await self.events.started.wait()

        while self.is_operational:
            peer, header = await self.wait(self._subscriber_notice)
            yield (peer, header)

    def register_peer(self, peer: BasePeer) -> None:
        # don't block the caller
        self.run_task(self._handle_new_peer(peer))

    async def _handle_new_peer(self, peer: BasePeer) -> None:
        if peer != self._peer_pool.highest_td_peer:
            self.logger.debug("%s connected but it doesn't have the latest block")
            return

        self.logger.debug("Connected to %s, asking for highest block", peer)
        block_hash = peer.head_hash

        # TODO: Maybe this header is already in the database, it's worth checking

        try:
            TWO_SECONDS = 2
            # using self.wait because even though peer will probably use the same cancel
            # token as this monitor was given it probably shouldn't rely on that
            headers = await self.wait(peer.requests.get_block_headers(
                block_hash, max_headers=1, reverse=False, timeout=TWO_SECONDS
            ))
        except (TimeoutError, PeerConnectionLost) as err:
            # TODO: maybe we should either retry or disconnect?
            self.logger.debug("Unable to fetch the latest block from %s, quitting", peer)
            return
        except ValidationError as err:
            self.logger.debug("Invalid header response from %s. Quitting", peer)
            return

        if peer != self._peer_pool.highest_td_peer:
            # another peer with a higher td likely connected while we were await'ing
            self.logger.debug("%s is no longer our highest peer, cancelling fetch", peer)
            return

        if len(headers) != 1:
            self.logger.debug(
                "Didn't expect to receive %s headers from peer. Quitting", len(headers)
            )
        else:
            header = headers[0]

        # TODO: validate the header and check the seal

        self.logger.info('notifying of new tip from %s, %s', peer, header)
        self._notify_tip(peer, header)

    def _handle_new_block(self, peer: BasePeer, msg: Dict[str, Any]) -> None:
        if peer != self._peer_pool.highest_td_peer:
            self.logger.debug("received new block from %s but it is not our highest peer", peer)
            return

        header, _, _ = msg['block']
        self.logger.info('new header %s from %s', header, peer)
        self._notify_tip(peer, header)

    async def _handle_msg_loop(self) -> None:
        while self.is_operational:
            peer, cmd, msg = await self.wait(self.msg_queue.get())
            if isinstance(cmd, commands.NewBlock):
                self._handle_new_block(peer, cast(Dict[str, Any], msg))
            else:
                assert False, 'unexpected message!'

    def _notify_tip(self, peer: BasePeer, header: BlockHeader) -> None:
        self._subscriber_notice.set_result((peer, header))
        self._subscriber_notice = self.get_event_loop().create_future()

    async def _run(self) -> None:
        self.run_daemon_task(self._handle_msg_loop())
        with self.subscribe(self._peer_pool):
            await self.wait(self.events.cancelled.wait())
