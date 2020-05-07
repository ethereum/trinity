import json

import pytest
import trio
from trio.testing import open_stream_to_socket_listener

from trinity._utils.trio_utils import JSONHTTPServer

TEST_TIMEOUT = 5
SOME_INFO = "some_info"


class TestContext:
    some_info = SOME_INFO


async def post_increment_handler(_context, request):
    return {"result": request["x"] + 1}


async def get_info_handler(context, request):
    return context.some_info + request["some-param"]


@pytest.mark.trio
async def test_trio_http_json_server_GET():
    method = "GET"
    some_param = 33
    path = "/info"
    api_handlers = {path: {method: get_info_handler}}
    context = TestContext()
    server = JSONHTTPServer(api_handlers, context)
    with trio.move_on_after(TEST_TIMEOUT):
        async with trio.open_nursery() as nursery:
            some_free_port = 0
            listeners = await nursery.start(
                trio.serve_tcp, server.handler, some_free_port
            )
            client_stream = await open_stream_to_socket_listener(listeners[0])

            request = (
                f"{method} {path}?some-param={some_param} HTTP/1.0\r\n\r\n"
            ).encode()
            await client_stream.send_all(request)
            response = bytes()
            async for chunk in client_stream:
                response += chunk
            response_body = response.decode("utf-8").split("\r\n\r\n")[-1]
            result = json.loads(response_body)
            assert result == SOME_INFO + str(some_param)
            nursery.cancel_scope.cancel()


@pytest.mark.trio
async def test_trio_http_json_server_POST():
    method = "POST"
    path = "/increment"
    api_handlers = {path: {method: post_increment_handler}}
    context = TestContext()
    server = JSONHTTPServer(api_handlers, context)
    with trio.move_on_after(TEST_TIMEOUT):
        async with trio.open_nursery() as nursery:
            some_free_port = 0
            listeners = await nursery.start(
                trio.serve_tcp, server.handler, some_free_port
            )
            client_stream = await open_stream_to_socket_listener(listeners[0])

            body = '{"x": 1}'
            request = (
                f"{method} {path} HTTP/1.0\r\nContent-Length: {len(body)}\r\n\r\n{body}"
            ).encode()
            await client_stream.send_all(request)
            response = bytes()
            async for chunk in client_stream:
                response += chunk
            response_body = response.decode("utf-8").split("\r\n\r\n")[-1]
            result = json.loads(response_body)
            assert result["result"] == 2
            nursery.cancel_scope.cancel()
