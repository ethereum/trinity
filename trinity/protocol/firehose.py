from typing import (
    Any,
    cast,
    Dict,
    FrozenSet,
    Optional,
    Type,
)

from rlp import sedes
from rlp.sedes import (
    BigEndianInt,
)

from p2p.exceptions import HandshakeFailure
from p2p.p2p_proto import DisconnectReason
from p2p.peer import BasePeer, BasePeerFactory
from p2p.peer_pool import BasePeerPool
from p2p.protocol import (
    BaseRequest,
    Command,
    _DecodedMsgType,
    PayloadType,
    Protocol,
)


from trinity.protocol.common.exchanges import BaseExchange
from trinity.protocol.common.handlers import BaseExchangeHandler
from trinity.protocol.common.normalizers import NoopNormalizer
from trinity.protocol.common.servers import BaseRequestServer
from trinity.protocol.common.trackers import BasePerformanceTracker
from trinity.protocol.common.validators import BaseValidator, noop_payload_validator


# Commands


class Status(Command):
    _cmd_id = 0
    structure = (
        ('protocol_version', sedes.big_endian_int),
    )


class GetStateData(Command):
    _cmd_id = 1
    structure = (
        ('request_id', sedes.big_endian_int),
    )


class StateData(Command):
    _cmd_id = 2
    structure = (
        ('request_id', sedes.big_endian_int),
    )


# Requests


class GetStateDataRequest(BaseRequest[BigEndianInt]):
    cmd_type = GetStateData
    response_type = StateData

    def __init__(self, request_id: int) -> None:
        self.command_payload = (request_id,)


# Trackers


class GetStateDataTracker(BasePerformanceTracker[GetStateDataRequest, BigEndianInt]):
    def _get_request_size(self, request: GetStateDataRequest) -> Optional[int]:
        return len(request.command_payload)

    def _get_result_size(self, result: BigEndianInt) -> int:
        return len(result)

    def _get_result_item_count(self, result: BigEndianInt) -> int:
        return len(result)


# Validators


class GetStateDataValidator(BaseValidator[None]):
    def validate_result(self, response: None) -> None:
        return


# Exchanges


class GetStateDataExchange(BaseExchange[BigEndianInt, BigEndianInt, BigEndianInt]):
    _normalizer = NoopNormalizer[BigEndianInt]()
    request_class = GetStateDataRequest
    tracker_class = GetStateDataTracker

    async def __call__(self, timeout: float = None) -> BigEndianInt:  # type: ignore
        validator = GetStateDataValidator()
        request = self.request_class(request_id=1)

        return await self.get_result(
            request,
            self._normalizer,
            validator,
            noop_payload_validator,
            timeout,
        )

# Handlers


class FirehoseExchangeHandler(BaseExchangeHandler):
    # TODO: you might not need to define this class
    _exchange_config = {
        'get_state_data': GetStateDataExchange,
    }

    get_state_data: GetStateDataExchange


# Protocol


class FirehoseProtocol(Protocol):
    name = 'firehose'
    version = 1
    _commands = (
        Status,
        GetStateData, StateData,
    )
    cmd_length = 3

    peer: 'FirehosePeer'

    def send_handshake(self) -> None:
        resp = {
            'protocol_version': self.version,
        }
        cmd = Status(self.cmd_id_offset, self.snappy_support)
        self.transport.send(*cmd.encode(resp))

    def send_get_state_data(self) -> None:
        cmd = GetStateData(self.cmd_id_offset, self.snappy_support)
        header, body = cmd.encode((1,))
        self.transport.send(header, body)

    def send_state_data(self, request_id: int) -> None:
        cmd = StateData(self.cmd_id_offset, self.snappy_support)
        header, body = cmd.encode((request_id,))
        self.transport.send(header, body)


# Peer


class FirehosePeer(BasePeer):
    supported_sub_protocols = (FirehoseProtocol,)
    sub_proto: FirehoseProtocol = None

    _requests: FirehoseExchangeHandler = None

    @property
    def requests(self) -> FirehoseExchangeHandler:
        if self._requests is None:
            self._requests = FirehoseExchangeHandler(self)
        return self._requests

    async def send_sub_proto_handshake(self) -> None:
        self.sub_proto.send_handshake()

    async def process_sub_proto_handshake(self, cmd: Command, msg: PayloadType) -> None:
        if not isinstance(cmd, Status):
            await self.disconnect(DisconnectReason.subprotocol_error)
            raise HandshakeFailure(f"Expected a status msg, got {cmd}, disconnecting")

        # TODO: fail if the remote is using the wrong version


class FirehosePeerFactory(BasePeerFactory):
    peer_class = FirehosePeer


class FirehosePeerPool(BasePeerPool):
    peer_factory_class = FirehosePeerFactory


# Servers


class FirehoseRequestServer(BaseRequestServer):
    subscription_msg_types: FrozenSet[Type[Command]] = frozenset({
        GetStateData, StateData,
    })

    async def _handle_msg(self, base_peer: BasePeer, cmd: Command,
                          msg: _DecodedMsgType) -> None:
        peer = cast(FirehosePeer, base_peer)

        if isinstance(cmd, GetStateData):
            msg = cast(Dict[str, Any], msg)
            request_id = cast(int, msg['request_id'])
            await self.handle_get_state_data(peer, request_id)

    async def handle_get_state_data(self, peer: FirehosePeer, request_id: int) -> None:
        peer.sub_proto.send_state_data(request_id)
