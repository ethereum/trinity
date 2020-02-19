import logging
import random
from types import (
    TracebackType,
)
from typing import (
    AsyncIterator,
    AsyncGenerator,
    Callable,
    Dict,
    Optional,
    Tuple,
    Type,
    TypeVar,
)

from async_generator import asynccontextmanager

import trio
from trio.abc import (
    ReceiveChannel,
    SendChannel,
)
from trio.hazmat import (
    checkpoint,
)

from eth_utils import (
    encode_hex,
)

from async_service import Service

from p2p.exceptions import (
    UnexpectedMessage,
)
from p2p.discv5.channel_services import (
    Endpoint,
    IncomingMessage,
    OutgoingMessage,
)
from p2p.discv5.abc import (
    ChannelHandlerSubscriptionAPI,
    NodeDBAPI,
    MessageDispatcherAPI,
)
from p2p.discv5.constants import (
    IP_V4_ADDRESS_ENR_KEY,
    MAX_REQUEST_ID,
    MAX_REQUEST_ID_ATTEMPTS,
    MAX_NODES_MESSAGE_TOTAL,
    UDP_PORT_ENR_KEY,
)
from p2p.discv5.messages import (
    BaseMessage,
    NodesMessage,
)
from p2p.discv5.typing import (
    NodeID,
)


def get_random_request_id() -> int:
    return random.randint(0, MAX_REQUEST_ID)


ChannelContentType = TypeVar("ChannelContentType")


class ChannelHandlerSubscription(ChannelHandlerSubscriptionAPI[ChannelContentType]):
    def __init__(self,
                 send_channel: SendChannel[ChannelContentType],
                 receive_channel: ReceiveChannel[ChannelContentType],
                 remove_fn: Callable[[], None],
                 ) -> None:
        self._send_channel = send_channel
        self.receive_channel = receive_channel
        self.remove_fn = remove_fn

    def cancel(self) -> None:
        self.remove_fn()

    async def __aenter__(self) -> "ChannelHandlerSubscription[ChannelContentType]":
        await self._send_channel.__aenter__()
        await self.receive_channel.__aenter__()
        return self

    async def __aexit__(self,
                        exc_type: Optional[Type[BaseException]],
                        exc_value: Optional[BaseException],
                        traceback: Optional[TracebackType],
                        ) -> None:
        self.remove_fn()
        await self._send_channel.__aexit__()
        await self.receive_channel.__aexit__()

    async def receive(self) -> ChannelContentType:
        return await self.receive_channel.receive()

    def __aiter__(self) -> AsyncIterator[ChannelContentType]:
        return self

    async def __anext__(self) -> ChannelContentType:
        try:
            return await self.receive()
        except trio.EndOfChannel:
            raise StopAsyncIteration


IncomingMessageSubscription = ChannelHandlerSubscription[IncomingMessage]


class MessageDispatcher(Service, MessageDispatcherAPI):
    logger = logging.getLogger("p2p.discv5.message_dispatcher.MessageDispatcher")

    def __init__(self,
                 node_db: NodeDBAPI,
                 incoming_message_receive_channel: ReceiveChannel[IncomingMessage],
                 outgoing_message_send_channel: SendChannel[OutgoingMessage],
                 ) -> None:
        self.node_db = node_db

        self.incoming_message_receive_channel = incoming_message_receive_channel
        self.outgoing_message_send_channel = outgoing_message_send_channel

        self.request_handler_send_channels: Dict[int, SendChannel[IncomingMessage]] = {}
        self.response_handler_send_channels: Dict[
            Tuple[NodeID, int],
            SendChannel[IncomingMessage],
        ] = {}

    async def run(self) -> None:
        async with self.incoming_message_receive_channel, self.outgoing_message_send_channel:
            async for incoming_message in self.incoming_message_receive_channel:
                await self.handle_incoming_message(incoming_message)

    async def handle_incoming_message(self, incoming_message: IncomingMessage) -> None:
        sender_node_id = incoming_message.sender_node_id
        message_type = incoming_message.message.message_type
        request_id = incoming_message.message.request_id

        is_request = message_type in self.request_handler_send_channels
        is_response = (sender_node_id, request_id) in self.response_handler_send_channels

        if is_request and is_response:
            self.logger.warning(
                f"%s from %s is both a response to an earlier request (id %d) and a request a "
                f"handler is present for (message type %d). Message will be handled twice.",
                incoming_message,
                encode_hex(sender_node_id),
                request_id,
                message_type,
            )
        if not is_request and not is_response:
            self.logger.warning(
                f"Dropping %s from %s (request id %d, message type %d) as neither a request nor a "
                f"response handler is present",
                incoming_message,
                encode_hex(sender_node_id),
                request_id,
                message_type,
            )
            await checkpoint()

        if is_request:
            self.logger.debug(
                "Received request %s with id %d from %s",
                incoming_message,
                request_id,
                encode_hex(sender_node_id),
            )
            send_channel = self.request_handler_send_channels[message_type]
            await send_channel.send(incoming_message)

        if is_response:
            self.logger.debug(
                "Received response %s for request with id %d from %s",
                incoming_message,
                request_id,
                encode_hex(sender_node_id),
            )
            send_channel = self.response_handler_send_channels[sender_node_id, request_id]
            await send_channel.send(incoming_message)

    def get_free_request_id(self, node_id: NodeID) -> int:
        for _ in range(MAX_REQUEST_ID_ATTEMPTS):
            request_id = get_random_request_id()
            if (node_id, request_id) not in self.response_handler_send_channels:
                return request_id
        else:
            # this should be extremely unlikely to happen
            raise ValueError(
                f"Failed to get free request id ({len(self.response_handler_send_channels)} "
                f"handlers added right now)"
            )

    def add_request_handler(self,
                            message_class: Type[BaseMessage],
                            ) -> IncomingMessageSubscription:
        message_type = message_class.message_type
        if message_type in self.request_handler_send_channels:
            raise ValueError(f"Request handler for {message_class.__name__} is already added")

        request_channels: Tuple[
            SendChannel[IncomingMessage],
            ReceiveChannel[IncomingMessage],
        ] = trio.open_memory_channel(0)
        self.request_handler_send_channels[message_type] = request_channels[0]

        self.logger.debug("Adding request handler for %s", message_class.__name__)

        def remove() -> None:
            try:
                self.request_handler_send_channels.pop(message_type)
            except KeyError:
                raise ValueError(
                    f"Request handler for {message_class.__name__} has already been removed"
                )
            else:
                self.logger.debug(
                    "Removing request handler for %s", message_class.__name__,
                )

        return ChannelHandlerSubscription(
            send_channel=request_channels[0],
            receive_channel=request_channels[1],
            remove_fn=remove,
        )

    def add_response_handler(self,
                             remote_node_id: NodeID,
                             request_id: int,
                             ) -> IncomingMessageSubscription:
        if (remote_node_id, request_id) in self.response_handler_send_channels:
            raise ValueError(
                f"Response handler for node id {encode_hex(remote_node_id)} and request id "
                f"{request_id} has already been added"
            )

        self.logger.debug(
            "Adding response handler for peer %s and request id %d",
            encode_hex(remote_node_id),
            request_id,
        )

        response_channels: Tuple[
            SendChannel[IncomingMessage],
            ReceiveChannel[IncomingMessage],
        ] = trio.open_memory_channel(0)
        self.response_handler_send_channels[(remote_node_id, request_id)] = response_channels[0]

        def remove() -> None:
            try:
                self.response_handler_send_channels.pop((remote_node_id, request_id))
            except KeyError:
                raise ValueError(
                    f"Response handler for node id {encode_hex(remote_node_id)} and request id "
                    f"{request_id} has already been removed"
                )
            else:
                self.logger.debug(
                    "Removing response handler for peer %s and request id %d",
                    encode_hex(remote_node_id),
                    request_id,
                )

        return ChannelHandlerSubscription(
            send_channel=response_channels[0],
            receive_channel=response_channels[1],
            remove_fn=remove,
        )

    async def get_endpoint_from_node_db(self, receiver_node_id: NodeID) -> Endpoint:
        try:
            enr = self.node_db.get_enr(receiver_node_id)
        except KeyError:
            raise ValueError(f"No ENR for peer {encode_hex(receiver_node_id)} known")

        try:
            ip_address = enr[IP_V4_ADDRESS_ENR_KEY]
        except KeyError:
            raise ValueError(
                f"ENR for peer {encode_hex(receiver_node_id)} does not contain an IP address"
            )

        try:
            udp_port = enr[UDP_PORT_ENR_KEY]
        except KeyError:
            raise ValueError(
                f"ENR for peer {encode_hex(receiver_node_id)} does not contain a UDP port"
            )

        return Endpoint(ip_address, udp_port)

    @asynccontextmanager
    async def request_response_subscription(self,
                                            receiver_node_id: NodeID,
                                            message: BaseMessage,
                                            endpoint: Optional[Endpoint] = None,
                                            ) -> AsyncGenerator[IncomingMessageSubscription, None]:
        if endpoint is None:
            endpoint = await self.get_endpoint_from_node_db(receiver_node_id)

        response_channels: Tuple[
            SendChannel[IncomingMessage],
            ReceiveChannel[IncomingMessage],
        ] = trio.open_memory_channel(0)
        response_send_channel, response_receive_channel = response_channels

        async with self.add_response_handler(
            receiver_node_id,
            message.request_id,
        ) as response_subscription:
            outgoing_message = OutgoingMessage(
                message=message,
                receiver_node_id=receiver_node_id,
                receiver_endpoint=endpoint,
            )
            self.logger.debug(
                "Sending %s to %s with request id %d",
                outgoing_message,
                encode_hex(receiver_node_id),
                message.request_id,
            )
            await self.outgoing_message_send_channel.send(outgoing_message)
            yield response_subscription

    async def request(self,
                      receiver_node_id: NodeID,
                      message: BaseMessage,
                      endpoint: Optional[Endpoint] = None,
                      ) -> IncomingMessage:
        async with self.request_response_subscription(
            receiver_node_id,
            message,
            endpoint,
        ) as response_subscription:
            response = await response_subscription.receive()
            self.logger.debug(
                "Received %s from %s with request id %d",
                response,
                encode_hex(receiver_node_id),
                message.request_id,
            )
            return response

    async def request_nodes(self,
                            receiver_node_id: NodeID,
                            message: BaseMessage,
                            endpoint: Optional[Endpoint] = None,
                            ) -> Tuple[IncomingMessage, ...]:
        async with self.request_response_subscription(
            receiver_node_id,
            message,
            endpoint,
        ) as response_subscription:
            first_response = await response_subscription.receive()
            self.logger.debug(
                "Received %s from %s with request id %d",
                first_response,
                encode_hex(receiver_node_id),
                message.request_id,
            )
            if not isinstance(first_response.message, NodesMessage):
                raise UnexpectedMessage(
                    f"Peer {encode_hex(receiver_node_id)} responded with "
                    f"{first_response.message.__class__.__name__} instead of Nodes message"
                )

            total = first_response.message.total
            if total > MAX_NODES_MESSAGE_TOTAL:
                raise UnexpectedMessage(
                    f"Peer {encode_hex(receiver_node_id)} sent nodes message with a total value of "
                    f"{total} which is too big"
                )
            self.logger.debug(
                "Received nodes response %d of %d from %s with request id %d",
                1,
                total,
                encode_hex(receiver_node_id),
                message.request_id,
            )

            responses = [first_response]
            for response_index in range(1, total):
                next_response = await response_subscription.receive()
                if not isinstance(first_response.message, NodesMessage):
                    raise UnexpectedMessage(
                        f"Peer {encode_hex(receiver_node_id)} responded with "
                        f"{next_response.message.__class__.__name__} instead of Nodes message"
                    )
                responses.append(next_response)
                self.logger.debug(
                    "Received nodes response %d of %d from %s with request id %d",
                    response_index + 1,
                    total,
                    encode_hex(receiver_node_id),
                    message.request_id,
                )
            return tuple(responses)
