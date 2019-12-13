import asyncio
import json
import logging
import pathlib
from typing import (
    Any,
    Callable,
    Tuple,
)

from async_service import Service
from eth_utils.toolz import curry

from trinity.rpc.main import (
    RPCServer,
)

MAXIMUM_REQUEST_BYTES = 10000
NEW_LINE = "\n"


logger = logging.getLogger('trinity.rpc.IPCServer')


@curry
async def connection_handler(execute_rpc: Callable[[Any], Any],
                             reader: asyncio.StreamReader,
                             writer: asyncio.StreamWriter) -> None:
    """
    Catch fatal errors, log them, and close the connection
    """

    try:
        await connection_loop(execute_rpc, reader, writer),
    except (ConnectionResetError, asyncio.IncompleteReadError):
        logger.debug("Client closed connection")
    except Exception:
        logger.exception("Unrecognized exception while handling requests")
    finally:
        writer.close()


async def connection_loop(execute_rpc: Callable[[Any], Any],
                          reader: asyncio.StreamReader,
                          writer: asyncio.StreamWriter) -> None:
    # TODO: we should look into using an io.StrinIO here for more efficient
    # writing to the end of the string.
    raw_request = ''
    while True:
        request_bytes = b''
        try:
            request_bytes = await reader.readuntil(b'}')
        except asyncio.LimitOverrunError as e:
            logger.info("Client request was too long. Erasing buffer and restarting...")
            request_bytes = await reader.read(e.consumed)
            await write_error(
                writer,
                f"reached limit: {e.consumed} bytes, starting with '{request_bytes[:20]!r}'",
            )
            continue

        raw_request += request_bytes.decode()

        bad_prefix, raw_request = strip_non_json_prefix(raw_request)
        if bad_prefix:
            logger.info("Client started request with non json data: %r", bad_prefix)
            await write_error(writer, 'Cannot parse json: ' + bad_prefix)

        try:
            request = json.loads(raw_request)
        except json.JSONDecodeError:
            # invalid json request, keep reading data until a valid json is formed
            logger.debug("Invalid JSON, waiting for rest of message: %r", raw_request)
            continue

        # reset the buffer for the next message
        raw_request = ''

        if not request:
            logger.debug("Client sent empty request")
            await write_error(writer, 'Invalid Request: empty')
            continue

        try:
            result = await execute_rpc(request)
        except Exception as e:
            logger.exception("Unrecognized exception while executing RPC")
            await write_error(writer, "unknown failure: " + str(e))
        else:
            if not result.endswith(NEW_LINE):
                result += NEW_LINE

            writer.write(result.encode())

        await writer.drain()


def strip_non_json_prefix(raw_request: str) -> Tuple[str, str]:
    if raw_request and raw_request[0] != '{':
        prefix, bracket, rest = raw_request.partition('{')
        return prefix.strip(), bracket + rest
    else:
        return '', raw_request


async def write_error(writer: asyncio.StreamWriter, message: str) -> None:
    json_error = json.dumps({'error': message})
    writer.write(json_error.encode())
    await writer.drain()


class IPCServer(Service):
    ipc_path = None
    rpc = None
    server = None

    logger = logger

    def __init__(
            self,
            rpc: RPCServer,
            ipc_path: pathlib.Path) -> None:
        self.rpc = rpc
        self.ipc_path = ipc_path

    async def run(self) -> None:
        server = await asyncio.start_unix_server(
            connection_handler(self.rpc.execute),
            str(self.ipc_path),
            limit=MAXIMUM_REQUEST_BYTES,
        )
        self.logger.info('IPC started at: %s', self.ipc_path.resolve())
        try:
            await self.manager.wait_finished()
        finally:
            server.close()
            self.ipc_path.unlink()
