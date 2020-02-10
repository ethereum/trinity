from typing import Union, TypeVar, Generic, Tuple

from cached_property import cached_property

from eth_typing import Hash32, BlockNumber
from eth_utils import encode_hex

from p2p.abc import MultiplexerAPI, ProtocolAPI, NodeAPI
from p2p.exceptions import (
    HandshakeFailure,
)
from p2p.handshake import Handshaker
from p2p.receipt import HandshakeReceipt

from trinity.exceptions import (
    WrongForkIDFailure,
    WrongGenesisFailure,
    WrongNetworkFailure,
    BaseForkIDValidationError,
)


from .commands import StatusV63, Status
from .forkid import ForkID, validate_forkid
from .payloads import StatusV63Payload, StatusPayload
from .proto import ETHProtocolV63, ETHProtocol


THandshakeParams = TypeVar("THandshakeParams", bound=Union[StatusPayload, StatusV63Payload])


class BaseETHHandshakeReceipt(HandshakeReceipt, Generic[THandshakeParams]):
    handshake_params: THandshakeParams

    def __init__(self, protocol: ProtocolAPI, handshake_params: THandshakeParams) -> None:
        super().__init__(protocol)
        self.handshake_params = handshake_params

    @cached_property
    def head_hash(self) -> Hash32:
        return self.handshake_params.head_hash

    @cached_property
    def genesis_hash(self) -> Hash32:
        return self.handshake_params.genesis_hash

    @cached_property
    def network_id(self) -> int:
        return self.handshake_params.network_id

    @cached_property
    def total_difficulty(self) -> int:
        return self.handshake_params.total_difficulty

    @cached_property
    def version(self) -> int:
        return self.handshake_params.version


class ETHV63HandshakeReceipt(BaseETHHandshakeReceipt[StatusV63Payload]):
    pass


class ETHHandshakeReceipt(BaseETHHandshakeReceipt[StatusPayload]):

    @cached_property
    def fork_id(self) -> ForkID:
        return self.handshake_params.fork_id


def validate_base_receipt(remote: NodeAPI,
                          receipt: Union[ETHV63HandshakeReceipt, ETHHandshakeReceipt],
                          handshake_params: Union[StatusV63Payload, StatusPayload]) -> None:
    if receipt.handshake_params.network_id != handshake_params.network_id:
        raise WrongNetworkFailure(
            f"{remote} network "
            f"({receipt.handshake_params.network_id}) does not match ours "
            f"({handshake_params.network_id}), disconnecting"
        )

    if receipt.handshake_params.genesis_hash != handshake_params.genesis_hash:
        raise WrongGenesisFailure(
            f"{remote} genesis "
            f"({encode_hex(receipt.handshake_params.genesis_hash)}) does "
            f"not match ours ({encode_hex(handshake_params.genesis_hash)}), "
            f"disconnecting"
        )


class ETHV63Handshaker(Handshaker[ETHProtocolV63]):
    protocol_class = ETHProtocolV63

    def __init__(self, handshake_params: StatusV63Payload) -> None:
        self.handshake_params = handshake_params

    async def do_handshake(self,
                           multiplexer: MultiplexerAPI,
                           protocol: ETHProtocolV63) -> ETHV63HandshakeReceipt:
        """
        Perform the handshake for the sub-protocol agreed with the remote peer.

        Raise HandshakeFailure if the handshake is not successful.
        """

        protocol.send(StatusV63(self.handshake_params))

        async for cmd in multiplexer.stream_protocol_messages(protocol):
            if not isinstance(cmd, StatusV63):
                raise HandshakeFailure(f"Expected a ETH Status msg, got {cmd}, disconnecting")

            receipt = ETHV63HandshakeReceipt(protocol, cmd.payload)

            validate_base_receipt(multiplexer.remote, receipt, self.handshake_params)

            break
        else:
            raise HandshakeFailure("Message stream exited before finishing handshake")

        return receipt


class ETHHandshaker(Handshaker[ETHProtocol]):
    protocol_class = ETHProtocol

    def __init__(self,
                 handshake_params: StatusPayload,
                 head_number: BlockNumber,
                 fork_blocks: Tuple[BlockNumber, ...]) -> None:
        self.handshake_params = handshake_params
        self.head_number = head_number
        self.fork_blocks = fork_blocks

    async def do_handshake(self,
                           multiplexer: MultiplexerAPI,
                           protocol: ETHProtocol) -> ETHHandshakeReceipt:
        """
        Perform the handshake for the sub-protocol agreed with the remote peer.

        Raise HandshakeFailure if the handshake is not successful.
        """

        protocol.send(Status(self.handshake_params))

        async for cmd in multiplexer.stream_protocol_messages(protocol):
            if not isinstance(cmd, Status):
                raise HandshakeFailure(f"Expected a ETH Status msg, got {cmd}, disconnecting")

            receipt = ETHHandshakeReceipt(protocol, cmd.payload)

            validate_base_receipt(multiplexer.remote, receipt, self.handshake_params)

            try:
                validate_forkid(
                    receipt.fork_id,
                    self.handshake_params.genesis_hash,
                    self.head_number,
                    self.fork_blocks,
                )
            except BaseForkIDValidationError as exc:
                raise WrongForkIDFailure(
                    f"{multiplexer.remote} forkid "
                    f"({receipt.handshake_params.fork_id}) is incompatible to ours ({exc})"
                    f"({self.handshake_params.fork_id}), disconnecting"
                )

            break
        else:
            raise HandshakeFailure("Message stream exited before finishing handshake")

        return receipt
