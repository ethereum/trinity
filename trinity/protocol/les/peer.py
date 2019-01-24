from abc import (
    abstractmethod,
)
from typing import (
    Any,
    cast,
    Dict,
    List,
    Union,
)
import typing_extensions

from eth_typing import (
    BlockNumber,
    Hash32,
)

from eth_utils import encode_hex
from lahja import (
    BroadcastConfig,
    Endpoint
)
from p2p.exceptions import (
    HandshakeFailure,
)
from p2p.peer import (
    IdentifiablePeer,
)
from p2p.p2p_proto import DisconnectReason
from p2p.protocol import (
    Command,
    _DecodedMsgType,
)

from trinity.protocol.common.peer import (
    BaseChainPeer,
    BaseChainPeerFactory,
    BaseChainPeerPool,
)
from trinity.protocol.common.peer_pool_event_bus import (
    BasePeerPoolEventBusRequestHandler,
)

from .commands import (
    Announce,
    Status,
    StatusV2,
)
from .constants import (
    MAX_HEADERS_FETCH,
)
from .events import (
    SendBlockHeadersEvent,
)
from .proto import (
    LESProtocol,
    LESProtocolLike,
    LESProtocolV2,
    ProxyLESProtocol,
)
from .handlers import LESExchangeHandler


class LESPeerLike(typing_extensions.Protocol):

    @property
    @abstractmethod
    def sub_proto(self) -> LESProtocolLike:
        pass

    @property
    @abstractmethod
    def is_operational(self) -> bool:
        pass


class LESPeer(BaseChainPeer):
    max_headers_fetch = MAX_HEADERS_FETCH

    _supported_sub_protocols = [LESProtocol, LESProtocolV2]
    sub_proto: LESProtocol = None

    _requests: LESExchangeHandler = None

    def get_extra_stats(self) -> List[str]:
        stats_pairs = self.requests.get_stats().items()
        return ['%s: %s' % (cmd_name, stats) for cmd_name, stats in stats_pairs]

    @property
    def requests(self) -> LESExchangeHandler:
        if self._requests is None:
            self._requests = LESExchangeHandler(self)
        return self._requests

    def handle_sub_proto_msg(self, cmd: Command, msg: _DecodedMsgType) -> None:
        head_info = cast(Dict[str, Union[int, Hash32, BlockNumber]], msg)
        if isinstance(cmd, Announce):
            self.head_td = cast(int, head_info['head_td'])
            self.head_hash = cast(Hash32, head_info['head_hash'])
            self.head_number = cast(BlockNumber, head_info['head_number'])

        super().handle_sub_proto_msg(cmd, msg)

    async def send_sub_proto_handshake(self) -> None:
        self.sub_proto.send_handshake(await self._local_chain_info)

    async def process_sub_proto_handshake(
            self, cmd: Command, msg: _DecodedMsgType) -> None:
        if not isinstance(cmd, (Status, StatusV2)):
            await self.disconnect(DisconnectReason.subprotocol_error)
            raise HandshakeFailure(f"Expected a LES Status msg, got {cmd}, disconnecting")
        msg = cast(Dict[str, Any], msg)
        if msg['networkId'] != self.network_id:
            await self.disconnect(DisconnectReason.useless_peer)
            raise HandshakeFailure(
                f"{self} network ({msg['networkId']}) does not match ours "
                f"({self.network_id}), disconnecting"
            )
        genesis = await self.genesis
        if msg['genesisHash'] != genesis.hash:
            await self.disconnect(DisconnectReason.useless_peer)
            raise HandshakeFailure(
                f"{self} genesis ({encode_hex(msg['genesisHash'])}) does not "
                f"match ours ({genesis.hex_hash}), disconnecting"
            )
        # Eventually we might want to keep connections to peers where we are the only side serving
        # data, but right now both our chain syncer and the Peer.boot() method expect the remote
        # to reply to header requests, so if they don't we simply disconnect here.
        if 'serveHeaders' not in msg:
            await self.disconnect(DisconnectReason.useless_peer)
            raise HandshakeFailure(f"{self} doesn't serve headers, disconnecting")
        self.head_td = msg['headTd']
        self.head_hash = msg['headHash']
        self.head_number = msg['headNum']


class LESProxyPeer:
    """
    A ``LESPeer`` that can be used from any process as a drop-in replacement for the actual
    peer that sits in the peer pool. Any action performed on the ``LESProxyPeer`` is delegated
    to the actual peer in the pool.
    """

    def __init__(self, sub_proto: ProxyLESProtocol):
        self.sub_proto = sub_proto

    @property
    def is_operational(self) -> bool:
        # We implement this API because parts of our code base works with actual and proxy peers
        # for the time being and expect this API to exist.
        # We return `True` here because it would be a waste to do this extra round trip, plus it
        # would only be a race condition at best.
        # When working with a proxy peer one *must* do the `is_operational` check in the request
        # handler that runs in the peer pool process.
        return True

    @classmethod
    def from_dto_peer(cls,
                      dto_peer: IdentifiablePeer,
                      event_bus: Endpoint,
                      broadcast_config: BroadcastConfig) -> 'LESProxyPeer':
        return cls(ProxyLESProtocol(dto_peer, event_bus, broadcast_config))


class LESPeerFactory(BaseChainPeerFactory):
    peer_class = LESPeer


class LESPeerPoolEventBusRequestHandler(BasePeerPoolEventBusRequestHandler[LESPeer]):
    """
    A request handler to handle LES specific requests to the peer pool.
    """

    async def _run(self) -> None:
        self.logger.info("Running LESPeerPoolEventBusRequestHandler")
        self.run_daemon_task(self.handle_send_blockheader_events())
        await super()._run()

    async def handle_send_blockheader_events(self) -> None:
        async for ev in self.wait_iter(self._event_bus.stream(SendBlockHeadersEvent)):
            peer = self.maybe_return_peer(ev.dto_peer)
            if peer is None:
                continue
            peer.sub_proto.send_block_headers(ev.headers, ev.buffer_value, ev.request_id)


class LESPeerPool(BaseChainPeerPool):
    peer_factory_class = LESPeerFactory
