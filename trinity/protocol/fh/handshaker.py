from typing import Tuple

from cached_property import cached_property

from eth_typing import BlockNumber, Hash32

from p2p.abc import MultiplexerAPI
from p2p.exceptions import (
    HandshakeFailure,
)
from p2p.handshake import Handshaker
from p2p.receipt import HandshakeReceipt

from trinity.protocol.eth.forkid import ForkID, validate_forkid
from trinity.exceptions import (
    WrongForkIDFailure,
    WrongNetworkFailure,
    BaseForkIDValidationError,
)

from .commands import Status, StatusPayload
from .proto import FirehoseProtocol


class FirehoseHandshakeReceipt(HandshakeReceipt):
    def __init__(
            self,
            protocol: FirehoseProtocol,
            handshake_params: StatusPayload) -> None:
        super().__init__(protocol)
        self.handshake_params = handshake_params

    @cached_property
    def network_id(self) -> int:
        return self.handshake_params.network_id

    @cached_property
    def fork_id(self) -> ForkID:
        return self.handshake_params.fork_id

    @cached_property
    def version(self) -> int:
        return self.handshake_params.version


class FirehoseHandshaker(Handshaker[FirehoseProtocol]):
    protocol_class = FirehoseProtocol

    def __init__(self,
                 handshake_params: StatusPayload,
                 genesis_hash: Hash32,
                 head_number: BlockNumber,
                 fork_blocks: Tuple[BlockNumber, ...]) -> None:
        self.handshake_params = handshake_params
        self.genesis_hash = genesis_hash
        self.head_number = head_number
        self.fork_blocks = fork_blocks

    async def do_handshake(self,
                           multiplexer: MultiplexerAPI,
                           protocol: FirehoseProtocol) -> FirehoseHandshakeReceipt:
        """Perform the handshake for the sub-protocol agreed with the remote peer.

        Raises HandshakeFailure if the handshake is not successful.
        """
        protocol.send(Status(self.handshake_params))

        async for cmd in multiplexer.stream_protocol_messages(protocol):
            if not isinstance(cmd, Status):
                raise HandshakeFailure(f"Expected a Firehose Status msg, got {cmd}, disconnecting")

            remote_params = StatusPayload(
                version=cmd.payload.version,
                network_id=cmd.payload.network_id,
                fork_id=cmd.payload.fork_id,
            )
            receipt = FirehoseHandshakeReceipt(protocol, remote_params)

            if receipt.handshake_params.network_id != self.handshake_params.network_id:
                raise WrongNetworkFailure(
                    f"{multiplexer.remote} network "
                    f"({receipt.handshake_params.network_id}) does not match ours "
                    f"({self.handshake_params.network_id}), disconnecting"
                )

            try:
                validate_forkid(
                    receipt.fork_id,
                    self.genesis_hash,
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
