from typing import (
    Any,
    Dict,
    FrozenSet,
    Type,
    cast,
)

from cancel_token import CancelToken
from lahja import (
    BroadcastConfig,
    Endpoint,
)

from p2p.peer import (
    BasePeer,
    IdentifiablePeer,
)
from p2p.protocol import (
    Command,
    _DecodedMsgType,
)

from trinity.db.eth1.header import BaseAsyncHeaderDB
from trinity.protocol.common.servers import (
    BaseRequestServer,
    BaseIsolatedRequestServer,
    BasePeerRequestHandler,
)
from trinity.protocol.les import commands
from trinity.protocol.les.peer import (
    LESPeer,
    LESPeerLike,
    LESPeerPool,
    LESProxyPeer,
)

from trinity.protocol.les.requests import HeaderRequest as LightHeaderRequest


class LESPeerRequestHandler(BasePeerRequestHandler):
    async def handle_get_block_headers(self, peer: LESPeerLike, msg: Dict[str, Any]) -> None:
        if not peer.is_operational:
            return
        self.logger.debug("Peer %s made header request: %s", peer, msg)
        request = LightHeaderRequest(
            msg['query'].block_number_or_hash,
            msg['query'].max_headers,
            msg['query'].skip,
            msg['query'].reverse,
            msg['request_id'],
        )
        headers = await self.lookup_headers(request)
        self.logger.debug2("Replying to %s with %d headers", peer, len(headers))
        peer.sub_proto.send_block_headers(headers, buffer_value=0, request_id=request.request_id)


class LightRequestServer(BaseRequestServer):
    """
    Monitor commands from peers, to identify inbound requests that should receive a response.
    Handle those inbound requests by querying our local database and replying.
    """
    subscription_msg_types: FrozenSet[Type[Command]] = frozenset({
        commands.GetBlockHeaders,
    })

    def __init__(
            self,
            db: BaseAsyncHeaderDB,
            peer_pool: LESPeerPool,
            token: CancelToken = None) -> None:
        super().__init__(peer_pool, token)
        self._handler = LESPeerRequestHandler(db, self.cancel_token)

    async def _handle_msg(self, base_peer: BasePeer, cmd: Command,
                          msg: _DecodedMsgType) -> None:
        peer = cast(LESPeer, base_peer)
        if isinstance(cmd, commands.GetBlockHeaders):
            block_request_kwargs = cast(Dict[str, Any], msg)
            await self._handler.handle_get_block_headers(peer, block_request_kwargs)
        else:
            self.logger.debug("%s msg from %s not implemented", cmd, peer)


class LightIsolatedRequestServer(BaseIsolatedRequestServer):
    """
    Like :class:`~trinity.protocol.les.servers.LightRequestServer` but can be run outside of the
    process that hosts the :class:`~p2p.peer_pool.BasePeerPool`.
    """

    _handled_commands = (
        commands.GetBlockHeaders,
    )

    def __init__(
            self,
            event_bus: Endpoint,
            broadcast_config: BroadcastConfig,
            db: BaseAsyncHeaderDB,
            token: CancelToken = None) -> None:
        super().__init__(event_bus, broadcast_config, token)
        self._handler = LESPeerRequestHandler(db, self.cancel_token)

    async def _handle_msg(self,
                          dto_peer: IdentifiablePeer,
                          cmd: Command,
                          msg: _DecodedMsgType) -> None:

        if type(cmd) not in self._handled_commands:
            return

        self.logger.debug("Peer %s requested %s", dto_peer.uri, cmd)
        peer = LESProxyPeer.from_dto_peer(dto_peer, self.event_bus, self.broadcast_config)
        if isinstance(cmd, commands.GetBlockHeaders):
            await self._handler.handle_get_block_headers(peer, cast(Dict[str, Any], msg))
        else:
            self.logger.debug("%s msg not handled yet, need to be implemented", cmd)
