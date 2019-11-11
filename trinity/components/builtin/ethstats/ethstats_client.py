import asyncio
import datetime
import json
import typing

import websockets

from eth_utils import get_extended_debug_logger

from p2p.service import Service


# Returns UTC timestamp in ms, used for latency calculation
def timestamp_ms() -> int:
    return round(datetime.datetime.utcnow().timestamp() * 1000)


EthstatsData = typing.Dict[str, typing.Any]


class EthstatsMessage(typing.NamedTuple):
    command: str
    data: EthstatsData


class EthstatsException(Exception):
    pass


class EthstatsClient(Service):
    logger = get_extended_debug_logger('trinity.components.ethstats.EthstatsClient')

    def __init__(
        self,
        websocket: websockets.client.WebSocketClientProtocol,
        node_id: str,
    ) -> None:
        self.websocket = websocket
        self.node_id = node_id

        self.send_queue: asyncio.Queue[EthstatsMessage] = asyncio.Queue()
        self.recv_queue: asyncio.Queue[EthstatsMessage] = asyncio.Queue()

    async def run(self) -> None:
        await asyncio.wait((
            self.send_handler(),
            self.recv_handler(),
        ), return_when=asyncio.FIRST_COMPLETED)
        self.manager.cancel()

    # Get messages from websocket, deserialize them and put into queue
    async def recv_handler(self) -> None:
        while self.manager.is_running:
            try:
                json_string: str = await self.websocket.recv()
            except websockets.ConnectionClosed as e:
                self.logger.debug2("Connection closed: %s", e)
                self.manager.cancel()
            try:
                message: EthstatsMessage = self.deserialize_message(json_string)
            except EthstatsException as e:
                self.logger.warning('Cannot parse message from server: %s' % e)
                return

            await self.recv_queue.put(message)

    # Get messages from queue, serialize them and send over websocket
    async def send_handler(self) -> None:
        while self.manager.is_running:
            message: EthstatsMessage = await self.send_queue.get()
            json_string: str = self.serialize_message(message)

            await self.websocket.send(json_string)

    def serialize_message(self, message: EthstatsMessage) -> str:
        return json.dumps({'emit': [
            message.command,
            {**message.data, 'id': self.node_id},
        ]})

    def deserialize_message(self, json_string: str) -> EthstatsMessage:
        try:
            raw_message = json.loads(json_string)
        except json.decoder.JSONDecodeError as e:
            raise EthstatsException('Received incorrect JSON: %s' % e)

        if isinstance(raw_message, str):
            raise EthstatsException(f'Received invalid payload: {raw_message}')

        try:
            payload = raw_message['emit']
        except KeyError:
            raise EthstatsException('Received incorrect payload')

        if len(payload) == 1:
            command, data = payload + [{}]
        elif len(payload) == 2:
            command, data = payload
        else:
            raise EthstatsException('Received non-ethstats payload')

        return EthstatsMessage(command, data)

    # Get received message from queue for processing
    async def recv(self) -> EthstatsMessage:
        return await self.recv_queue.get()

    # Following methods used to enqueue messages to be sent

    async def send_hello(self, secret: str, info: EthstatsData) -> None:
        await self.send_queue.put(EthstatsMessage(
            'hello',
            {'info': info, 'secret': secret},
        ))

    async def send_stats(self, stats: EthstatsData) -> None:
        await self.send_queue.put(EthstatsMessage(
            'stats',
            {'stats': stats},
        ))

    async def send_block(self, block: EthstatsData) -> None:
        await self.send_queue.put(EthstatsMessage(
            'block',
            {'block': block},
        ))

    async def send_pending(self, pending: int) -> None:
        await self.send_queue.put(EthstatsMessage(
            'pending',
            {'stats': {'pending': pending}},
        ))

    async def send_history(self, history: EthstatsData) -> None:
        await self.send_queue.put(EthstatsMessage(
            'history',
            {'history': history},
        ))

    async def send_node_ping(self) -> None:
        await self.send_queue.put(EthstatsMessage(
            'node-ping',
            {'clientTime': timestamp_ms()},
        ))

    async def send_latency(self, latency: int) -> None:
        await self.send_queue.put(EthstatsMessage(
            'latency',
            {'latency': latency},
        ))
