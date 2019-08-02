from abc import (
    ABC,
    abstractmethod,
)
from contextlib import asynccontextmanager
import logging
from typing import (
    AsyncGenerator,
    Optional,
    Type,
)

import trio
from trio.abc import (
    ReceiveChannel,
    SendChannel,
)

from eth_utils import (
    encode_hex,
    ValidationError,
)

from eth_typing import (
    Hash32,
)

from p2p.trio_service import Service

from p2p.exceptions import (
    DecryptionError,
    HandshakeFailure,
)

from p2p.discv5.enr import ENR
from p2p.discv5.identity_schemes import IdentityScheme
from p2p.discv5.packets import (
    AuthHeaderPacket,
    AuthTagPacket,
    WhoAreYouPacket,
    get_random_auth_tag,
    get_random_encrypted_data,
    get_random_id_nonce,
)
from p2p.discv5.tags import (
    compute_tag,
)
from p2p.discv5.typing import (
    AES128Key,
    IncomingMessage,
    IncomingPacket,
    Nonce,
    OutgoingMessage,
    OutgoingPacket,
    SessionKeys,
)


class BaseHandshakeService(Service, ABC):

    def __init__(self,
                 peer_node_id: bytes,
                 local_enr: ENR,
                 local_private_key: bytes,
                 incoming_packet_receive_channel: ReceiveChannel[IncomingPacket],
                 outgoing_packet_send_channel: SendChannel[OutgoingPacket],
                 session_keys_send_channel: SendChannel[SessionKeys],
                 ) -> None:
        self.logger = logging.getLogger("p2p.discv5.handshake." + self.__class__.__name__)

        self.peer_node_id = peer_node_id

        self.local_enr = local_enr
        self.local_private_key = local_private_key

        self.incoming_packet_receive_channel = incoming_packet_receive_channel
        self.outgoing_packet_send_channel = outgoing_packet_send_channel

        self.session_keys_send_channel = session_keys_send_channel

    async def drop_incoming_packets(self) -> None:
        async for incoming_packet in self.incoming_packet_receive_channel:
            self.logger.debug(
                f"Dropping {incoming_packet.__class__.__name__} from "
                f"{encode_hex(self.peer_node_id)} as handshake is currently in progress"
            )
            continue

    @asynccontextmanager
    async def while_dropping_incoming_packets(self) -> AsyncGenerator[None, None]:
        """Context manager to ignore incoming packets while working on other tasks."""
        async with trio.open_nursery() as nursery:
            nursery.start_soon(self.drop_incoming_packets)
            try:
                yield
            finally:
                nursery.cancel_scope.cancel()

    @abstractmethod
    @property
    def identity_scheme(self) -> Type[IdentityScheme]:
        ...

    @property
    def tag(self) -> Hash32:
        return compute_tag(
            source_node_id=self.local_enr.node_id,
            destination_node_id=self.peer_node_id,
        )


class HandshakeInitiator(BaseHandshakeService):

    def __init__(self,
                 *,
                 peer_enr: ENR,
                 local_enr: ENR,
                 local_private_key: bytes,
                 initial_message: OutgoingMessage,
                 incoming_packet_receive_channel: ReceiveChannel[IncomingPacket],
                 outgoing_packet_send_channel: SendChannel[OutgoingPacket],
                 session_keys_send_channel: SendChannel[SessionKeys],
                 ) -> None:
        self.peer_enr = peer_enr
        self.peer_node_id = self.peer_enr.node_id
        self.local_enr = local_enr

        self.initial_message = initial_message

        self.incoming_packet_receive_channel = incoming_packet_receive_channel
        self.outgoing_packet_send_channel = outgoing_packet_send_channel

        self.session_keys_send_channel = session_keys_send_channel

    @property
    def identity_scheme(self) -> Type[IdentityScheme]:
        return self.peer_enr.identity_scheme

    async def run(self) -> None:
        self.logger.info(f"Initiating handshake with {encode_hex(self.peer_node_id)}")

        async with self.incoming_packet_receive_channel, self.outgoing_packet_send_channel:
            async with self.while_dropping_incoming_packets():
                token = await self.send_initiating_packet()

            # wait for the who are you response
            incoming_packet = await self.receive_who_are_you_response(token)
            who_are_you_packet = incoming_packet.packet

            # compute session keys
            ephemeral_private_key, ephemeral_public_key = self.identity_scheme.get_random_key_pair()
            session_keys = self.identity_scheme.compute_session_keys(
                local_private_key=ephemeral_private_key,
                peer_public_key=self.peer_enr.public_key,
                initiator_node_id=self.local_enr.node_id,
                recipient_node_id=self.peer_node_id,
                id_nonce=who_are_you_packet.id_nonce,
            )

            # send auth header response and finalize handshake
            async with self.while_dropping_incoming_packets():
                await self.send_auth_header_packet(
                    who_are_you_packet,
                    session_keys,
                    ephemeral_public_key,
                )
                await self.session_keys_send_channel.send(session_keys)

        self.logger.info(f"Successfully completed handshake with {encode_hex(self.peer_node_id)}")

    async def send_initiating_packet(self) -> None:
        self.logger.debug(
            f"Sending random data to {encode_hex(self.peer_node_id)} to trigger WhoAreYou response"
        )

        auth_tag_packet = AuthTagPacket.prepare_random(
            tag=self.tag,
            auth_tag=get_random_auth_tag(),
            random_data=get_random_encrypted_data()
        )
        outgoing_packet = OutgoingPacket(
            packet=auth_tag_packet,
            receiver=self.initial_message.receiver,
        )
        await self.outgoing_packet_send_channel.send(outgoing_packet)

    async def receive_who_are_you_response(self, token: Nonce) -> IncomingPacket:
        self.logger.debug("Waiting for WhoAreYou packet from {encode_hex(self.peer_node_id)}")

        async for incoming_packet in self.incoming_packet_receive_channel:
            packet = incoming_packet.packet
            if isinstance(packet, WhoAreYouPacket):
                if packet.token == token:
                    self.logger.debug(
                        f"Received expected WhoAreYou response from {encode_hex(self.peer_node_id)}"
                    )
                    return incoming_packet
                else:
                    self.logger.warning(
                        f"Received WhoAreYou packet from {encode_hex(self.peer_node_id)}, but the "
                        f"token {encode_hex(packet.token)} does not match the auth tag of the "
                        f"initating packet ({encode_hex(token)})."
                    )
                    continue
            else:
                self.logger.debug(
                    f"Dropping {incoming_packet.__class__.__name__} from "
                    f"{encode_hex(self.peer_node_id)} since we are awaiting a WhoAreYou packet"
                )
                continue

    async def send_auth_header_packet(self,
                                      who_are_you_packet: WhoAreYouPacket,
                                      session_keys: SessionKeys,
                                      ephemeral_public_key: bytes,
                                      ) -> None:
        id_nonce_signature = self.identity_scheme.create_id_nonce_signature(
            id_nonce=who_are_you_packet.id_nonce,
            private_key=self.local_private_key,
        )

        if who_are_you_packet.enr_sequence_number < self.local_enr.sequence_number:
            enr = self.local_enr
        else:
            enr = None

        auth_header_packet = AuthHeaderPacket.prepare(
            tag=self.tag,
            auth_tag=get_random_auth_tag(),
            message=self.initial_message,
            initiator_key=session_keys.initator_key,
            id_nonce_signature=id_nonce_signature,
            auth_response_key=session_keys.auth_response_key,
            enr=enr,
            ephemeral_pubkey=ephemeral_public_key,
        )

        self.logger.debug(f"Sending AuthHeaderPacket to {encode_hex(self.peer_node_id)}")
        await self.outgoing_packet_send_channel.send(auth_header_packet)


class HandshakeRecipient(BaseHandshakeService):

    def __init__(self,
                 *,
                 peer_node_id: bytes,
                 peer_enr: Optional[ENR],
                 local_enr: ENR,
                 local_private_key: bytes,
                 initiating_packet: IncomingPacket,
                 incoming_packet_receive_channel: ReceiveChannel[IncomingPacket],
                 outgoing_packet_send_channel: SendChannel[OutgoingPacket],
                 session_keys_send_channel: SendChannel[SessionKeys],
                 enr_update_send_channel: SendChannel[ENR],
                 incoming_message_send_channel: SendChannel[IncomingMessage],
                 ) -> None:
        super().__init__(
            peer_node_id=peer_node_id,
            local_enr=local_enr,
            local_private_key=local_private_key,
            incoming_packet_receive_channel=incoming_packet_receive_channel,
            outgoing_packet_send_channel=outgoing_packet_send_channel,
            session_keys_send_channel=session_keys_send_channel,
        )

        if peer_enr is not None:
            if peer_enr.node_id != peer_node_id:
                raise ValueError("Node id according to ENR does not match explicitly given one")

        self.peer_enr = peer_enr
        self.enr_update_send_channel = enr_update_send_channel
        self.incoming_message_send_channel = incoming_message_send_channel

        self.initiating_packet = initiating_packet
        if not isinstance(self.initiating_packet.packet, AuthTagPacket):
            raise TypeError("Handshake must be initiated by AuthTagPacket")

    @property
    def identity_scheme(self) -> Type[IdentityScheme]:
        return self.local_enr.identity_scheme

    async def run(self) -> None:
        self.logger.info(f"Starting handshake as recipient with {encode_hex(self.peer_node_id)}")

        async with (
            self.incoming_packet_receive_channel,
            self.outgoing_packet_send_channel,
            self.enr_update_send_channel,
            self.session_keys_send_channel,
            self.incoming_message_send_channel,
        ):
            async with self.while_dropping_incoming_packets():
                id_nonce = await self.send_who_are_you_response()

            incoming_packet = await self.receive_auth_header_response()
            auth_header_packet = incoming_packet.packet

            ephemeral_public_key = auth_header_packet.auth_header.ephemeral_public_key
            try:
                self.identity_scheme.validate_public_key(ephemeral_public_key)
            except ValidationError as error:
                raise HandshakeFailure(
                    f"AuthHeader packet from contains invalid ephemeral public key "
                    f"{encode_hex(ephemeral_public_key)}"
                ) from error

            session_keys = self.identity_scheme.compute_session_keys(
                local_private_key=self.local_private_key,
                peer_public_key=ephemeral_public_key,
                initiator_node_id=self.peer_node_id,
                recipient_node_id=self.local_enr.node_id,
                id_nonce=id_nonce,
            )

            enr = self.decrypt_and_validate_auth_response(
                auth_header_packet,
                session_keys.auth_response_key,
                id_nonce,
            )

            try:
                message = auth_header_packet.decrypt(session_keys.initiator_key)
            except DecryptionError as error:
                raise HandshakeFailure(
                    "Failed to decrypt message with newly established session keys"
                ) from error
            except ValidationError as error:
                raise HandshakeFailure("Received invalid message") from error
            else:
                incoming_message = IncomingMessage(
                    message=message,
                    sender=incoming_packet.sender,
                    node_id=self.peer_node_id,
                )

            self.logger.info(
                f"Successfully completed handshake with {encode_hex(self.peer_node_id)}"
            )
            async with self.while_dropping_incoming_packets():
                if enr is not None:
                    await self.enr_update_send_channel.send(enr)
                await self.session_keys_send_channel.send(session_keys)
                await self.incoming_message_send_channel.send(incoming_message)

    def decrypt_and_validate_auth_response(self,
                                           auth_header_packet: AuthHeaderPacket,
                                           auth_response_key: AES128Key,
                                           id_nonce: bytes,
                                           ) -> Optional[ENR]:
        # decrypt
        try:
            id_nonce_signature, enr = auth_header_packet.decrypt_auth_response(auth_response_key)
        except DecryptionError as error:
            raise HandshakeFailure("Unable to decrypt auth response") from error
        except ValidationError as error:
            raise HandshakeFailure("Invalid auth response content") from error

        # validate ENR if present
        if enr is None:
            if self.peer_enr is None:
                raise HandshakeFailure("Peer failed to send their ENR")
            else:
                current_peer_enr = self.peer_enr
        else:
            try:
                enr.validate_signature()
            except ValidationError as error:
                raise HandshakeFailure("ENR in auth response contains invalid signature") from error

            if enr.sequence_number <= self.peer_enr.sequence_number:
                raise HandshakeFailure(
                    "ENR in auth response is not newer than what we already have"
                )

            if enr.node_id != self.peer_node_id:
                raise HandshakeFailure(
                    f"ENR received from peer belongs to different node ({encode_hex(enr.node_id)} "
                    f"instead of {encode_hex(self.peer_node_id)})"
                )

            current_peer_enr = enr

        # validate id nonce signature
        try:
            self.identity_scheme.validate_id_nonce_signature(
                signature=id_nonce_signature,
                id_nonce=id_nonce,
                public_key=current_peer_enr.public_key,
            )
        except ValidationError as error:
            raise HandshakeFailure("Invalid id nonce signature in auth response") from error

        return enr

    async def send_who_are_you_response(self) -> bytes:
        id_nonce = get_random_id_nonce()

        if self.peer_enr is None:
            enr_sequence_number = 0
        else:
            enr_sequence_number = self.peer_enr.sequence_number

        who_are_you_packet = WhoAreYouPacket.prepare(
            tag=self.tag,
            destination_node_id=self.peer_node_id,
            token=self.initiating_packet.packet.auth_tag,
            id_nonce=id_nonce,
            enr_sequence_number=enr_sequence_number,
        )
        outgoing_packet = OutgoingPacket(
            packet=who_are_you_packet,
            receiver=self.initiating_packet.sender,
        )
        self.logger.debug(f"Sending WhoAreYou response to {encode_hex(self.peer_node_id)}")
        await self.outgoing_packet_send_channel.send(outgoing_packet)

        return id_nonce

    async def receive_auth_header_response(self) -> IncomingPacket:
        self.logger.debug("Waiting for AuthHeaderPacket from {encode_hex(self.peer_node_id)}")

        async for incoming_packet in self.incoming_packet_receive_channel:
            packet = incoming_packet.packet
            if isinstance(packet, AuthHeaderPacket):
                self.logger.debug(
                    f"Received expected AuthHeaderPacket response from "
                    f"{encode_hex(self.peer_node_id)}"
                )
                return incoming_packet
            else:
                self.logger.debug(
                    f"Dropping {packet.__class__.__name__} from {encode_hex(self.peer_node_id)} "
                    f"since we are awaiting an AuthHeader packet"
                )
                continue
