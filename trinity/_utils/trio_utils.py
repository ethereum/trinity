from itertools import count
import json
import logging
import signal
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Generic,
    Sequence,
    Tuple,
    TypeVar,
    Union,
)
from urllib.parse import urlparse, parse_qsl
from wsgiref.handlers import format_date_time

from eth_utils.toolz import curry
import h11
import trio
from trio_typing import TaskStatus


async def wait_first(callables: Sequence[Callable[[], Awaitable[Any]]]) -> None:
    """
    Run any number of tasks but cancel out any outstanding tasks as soon as the first one finishes.
    """
    async with trio.open_nursery() as nursery:
        for task in callables:
            async def _run_then_cancel() -> None:
                await task()
                nursery.cancel_scope.cancel()
            nursery.start_soon(_run_then_cancel)


async def wait_for_interrupts() -> None:
    with trio.open_signal_receiver(signal.SIGINT, signal.SIGTERM) as stream:
        async for _ in stream:
            return


# NOTE: Adatped from https://github.com/python-hyper/h11/blob/14f6185/examples/trio-server.py
# A simple HTTP server implemented using h11 and Trio:
#   http://trio.readthedocs.io/en/latest/index.html
# (so requires python 3.5+).
#
# All requests get echoed back a JSON document containing information about
# the request.
#
# This is a rather involved example, since it attempts to both be
# fully-HTTP-compliant and also demonstrate error handling.
#
# The main difference between an HTTP client and an HTTP server is that in a
# client, if something goes wrong, you can just throw away that connection and
# make a new one. In a server, you're expected to handle all kinds of garbage
# input and internal errors and recover with grace and dignity. And that's
# what this code does.
#
# I recommend pushing on it to see how it works -- e.g. watch what happens if
# you visit http://localhost:8080 in a webbrowser that supports keep-alive,
# hit reload a few times, and then wait for the keep-alive to time out on the
# server.
#
# Or try using curl to start a chunked upload and then hit control-C in the
# middle of the upload:
#
#    (for CHUNK in $(seq 10); do echo $CHUNK; sleep 1; done) \
#      | curl -T - http://localhost:8080/foo
#
# (Note that curl will send Expect: 100-Continue, too.)
#
# Or, heck, try letting curl complete successfully ;-).

# Some potential improvements, if you wanted to try and extend this to a real
# general-purpose HTTP server (and to give you some hints about the many
# considerations that go into making a robust HTTP server):
#
# - The timeout handling is rather crude -- we impose a flat 10 second timeout
#   on each request (starting from the end of the previous
#   response). Something finer-grained would be better. Also, if a timeout is
#   triggered we unconditionally send a 500 Internal Server Error; it would be
#   better to keep track of whether the timeout is the client's fault, and if
#   so send a 408 Request Timeout.
#
# - The error handling policy here is somewhat crude as well. It handles a lot
#   of cases perfectly, but there are corner cases where the ideal behavior is
#   more debateable. For example, if a client starts uploading a large
#   request, uses 100-Continue, and we send an error response, then we'll shut
#   down the connection immediately (for well-behaved clients) or after
#   spending TIMEOUT seconds reading and discarding their upload (for
#   ill-behaved ones that go on and try to upload their request anyway). And
#   for clients that do this without 100-Continue, we'll send the error
#   response and then shut them down after TIMEOUT seconds. This might or
#   might not be your preferred policy, though -- maybe you want to shut such
#   clients down immediately (even if this risks their not seeing the
#   response), or maybe you're happy to let them continue sending all the data
#   and wasting your bandwidth if this is what it takes to guarantee that they
#   see your error response. Up to you, really.
#
# - Another example of a debateable choice: if a response handler errors out
#   without having done *anything* -- hasn't started responding, hasn't read
#   the request body -- then this connection actually is salvagable, if the
#   server sends an error response + reads and discards the request body. This
#   code sends the error response, but it doesn't try to salvage the
#   connection by reading the request body, it just closes the
#   connection. This is quite possibly the best option, but again this is a
#   policy decision.
#
# - Our error pages always include the exception text. In real life you might
#   want to log the exception but not send that information to the client.
#
# - Our error responses perhaps should include Connection: close when we know
#   we're going to close this connection.
#
# - We don't support the HEAD method, but ought to.
#
# - We should probably do something cleverer with buffering responses and
#   TCP_CORK and suchlike.

MAX_RECV = 2 ** 16
TIMEOUT = 10

################################################################
# I/O adapter: h11 <-> trio
################################################################

# The core of this could be factored out to be usable for trio-based clients
# too, as well as servers. But as a simplified pedagogical example we don't
# attempt this here.


class TrioHTTPWrapper:
    logger = logging.getLogger("trinity._utils.trio_utils.TrioHTTPWrapper")

    _next_id = count()

    def __init__(self, stream: trio.SocketStream):
        self.stream = stream
        self.conn = h11.Connection(h11.SERVER)
        # Our Server: header
        self.ident = " ".join(
            ["h11-example-trio-server/{}".format(h11.__version__), h11.PRODUCT_ID]
        ).encode("ascii")
        # A unique id for this connection, to include in debugging output
        # (useful for understanding what's going on if there are multiple
        # simultaneous clients).
        self._obj_id = next(TrioHTTPWrapper._next_id)

    async def send(self, event: h11._events._EventBundle) -> None:
        # The code below doesn't send ConnectionClosed, so we don't bother
        # handling it here either -- it would require that we do something
        # appropriate when 'data' is None.
        assert type(event) is not h11.ConnectionClosed
        data = self.conn.send(event)
        await self.stream.send_all(data)

    async def _read_from_peer(self) -> None:
        if self.conn.they_are_waiting_for_100_continue:
            go_ahead = h11.InformationalResponse(
                status_code=100, headers=self.basic_headers()
            )
            await self.send(go_ahead)
        try:
            data = await self.stream.receive_some(MAX_RECV)
        except ConnectionError:
            # They've stopped listening. Not much we can do about it here.
            data = b""
        self.conn.receive_data(data)

    async def next_event(self) -> h11._events._EventBundle:
        while True:
            event = self.conn.next_event()
            if event is h11.NEED_DATA:
                await self._read_from_peer()
                continue
            return event

    async def shutdown_and_clean_up(self) -> None:
        # When this method is called, it's because we definitely want to kill
        # this connection, either as a clean shutdown or because of some kind
        # of error or loss-of-sync bug, and we no longer care if that violates
        # the protocol or not. So we ignore the state of self.conn, and just
        # go ahead and do the shutdown on the socket directly. (If you're
        # implementing a client you might prefer to send ConnectionClosed()
        # and let it raise an exception if that violates the protocol.)
        #
        try:
            await self.stream.send_eof()
        except trio.BrokenResourceError:
            # They're already gone, nothing to do
            return
        # Wait and read for a bit to give them a chance to see that we closed
        # things, but eventually give up and just close the socket.
        # XX FIXME: possibly we should set SO_LINGER to 0 here, so
        # that in the case where the client has ignored our shutdown and
        # declined to initiate the close themselves, we do a violent shutdown
        # (RST) and avoid the TIME_WAIT?
        # it looks like nginx never does this for keepalive timeouts, and only
        # does it for regular timeouts (slow clients I guess?) if explicitly
        # enabled ("Default: reset_timedout_connection off")
        with trio.move_on_after(TIMEOUT):
            try:
                while True:
                    # Attempt to read until EOF
                    got = await self.stream.receive_some(MAX_RECV)
                    if not got:
                        break
            except trio.BrokenResourceError:
                pass
            finally:
                await self.stream.aclose()

    def basic_headers(self) -> Tuple[Tuple[str, bytes], ...]:
        # HTTP requires these headers in all responses (client would do
        # something different here)
        return (
            ("Date", format_date_time(None).encode("ascii")),
            ("Server", self.ident),
        )

    def info(self, *args: Any) -> None:
        encoded_args = map(str, args)
        self.logger.debug("conn %d: %s", self._obj_id, " ".join(encoded_args))


################################################################
# Server main loop
################################################################

# General theory:
#
# If everything goes well:
# - we'll get a Request
# - our response handler will read the request body and send a full response
# - that will either leave us in MUST_CLOSE (if the client doesn't
#   support keepalive) or DONE/DONE (if the client does).
#
# But then there are many, many different ways that things can go wrong
# here. For example:
# - we don't actually get a Request, but rather a ConnectionClosed
# - exception is raised from somewhere (naughty client, broken
#   response handler, whatever)
#   - depending on what went wrong and where, we might or might not be
#     able to send an error response, and the connection might or
#     might not be salvagable after that
# - response handler doesn't fully read the request or doesn't send a
#   full response
#
# But these all have one thing in common: they involve us leaving the
# nice easy path up above. So we can just proceed on the assumption
# that the nice easy thing is what's happening, and whenever something
# goes wrong do our best to get back onto that path, and h11 will keep
# track of how successful we were and raise new errors if things don't work
# out.

Path = str
Method = str

TContext = TypeVar("TContext")

JSON = Union[Dict[str, Any], str, int, bool, Sequence[Any]]
Request = JSON
Response = JSON
Handler = Callable[[TContext, Request], Awaitable[Response]]
Router = Callable[[Method, Path], Handler[TContext]]


@curry
async def http_serve_json_api(
    router: Router[TContext], context: TContext, stream: trio.SocketStream
) -> None:
    wrapper = TrioHTTPWrapper(stream)
    while True:
        assert wrapper.conn.states == {h11.CLIENT: h11.IDLE, h11.SERVER: h11.IDLE}

        try:
            with trio.fail_after(TIMEOUT):
                event = await wrapper.next_event()
                if type(event) is h11.Request:
                    await send_json_response(router, wrapper, event, context)
        except Exception as exc:
            await maybe_send_error_response(wrapper, exc)

        if wrapper.conn.our_state is h11.MUST_CLOSE:
            await wrapper.shutdown_and_clean_up()
            return
        else:
            try:
                wrapper.conn.start_next_cycle()
            except h11.ProtocolError:
                states = wrapper.conn.states
                await maybe_send_error_response(
                    wrapper, RuntimeError("unexpected state {}".format(states))
                )
                await wrapper.shutdown_and_clean_up()
                return


################################################################
# Actual response handlers
################################################################

# Helper function
async def send_simple_response(
    wrapper: TrioHTTPWrapper,
    status_code: int,
    content_type: str,
    body: bytes,
    response_headers: Tuple[Tuple[str, str], ...] = None,
) -> None:
    headers = wrapper.basic_headers()
    headers += (("Content-Type", content_type),)
    headers += (("Content-Length", str(len(body))),)
    if response_headers:
        headers += (response_headers,)
    res = h11.Response(status_code=status_code, headers=headers)
    await wrapper.send(res)
    await wrapper.send(h11.Data(data=body))
    await wrapper.send(h11.EndOfMessage())


async def maybe_send_error_response(wrapper: TrioHTTPWrapper, exc: Exception) -> None:
    # If we can't send an error, oh well, nothing to be done
    if wrapper.conn.our_state not in {h11.IDLE, h11.SEND_RESPONSE}:
        return
    try:
        if isinstance(exc, h11.RemoteProtocolError):
            status_code = exc.error_status_hint
        elif isinstance(exc, trio.TooSlowError):
            status_code = 408  # Request Timeout
        elif isinstance(exc, ResourceNotFoundException):
            await send_simple_response(
                wrapper,
                exc.error_status_hint,
                "text/html; charset=UTF-8",
                b"Resource not found",
            )
            return
        elif isinstance(exc, MethodNotAllowedException):
            await send_simple_response(
                wrapper,
                exc.error_status_hint,
                "text/html; charset=UTF-8",
                b"",
                (("Allow", "GET, POST"),),
            )
            return
        else:
            status_code = 500
        body = str(exc).encode("utf-8")
        await send_simple_response(
            wrapper, status_code, "text/plain; charset=utf-8", body
        )
    except Exception as exc:
        wrapper.info("error while sending error response:", exc)


class ResourceNotFoundException(Exception):
    error_status_hint = 404


class MethodNotAllowedException(Exception):
    error_status_hint = 405


Handlers = Dict[Path, Dict[Method, Handler[TContext]]]


def make_router(handlers: Handlers[TContext]) -> Router[TContext]:
    def _router(path: Path, method: Method) -> Handler[TContext]:
        if path not in handlers:
            raise ResourceNotFoundException()
        handlers_for_path = handlers[path]

        if method not in handlers_for_path:
            raise MethodNotAllowedException()

        handler = handlers_for_path[method]
        return handler

    return _router


async def send_json_response(
    router: Router[TContext],
    wrapper: TrioHTTPWrapper,
    request: h11._events._EventBundle,
    context: TContext,
) -> None:
    if request.method not in {b"GET", b"POST"}:
        # Laziness: we should send a proper 405 Method Not Allowed with the
        # appropriate Accept: header, but we don't.
        return

    target = urlparse(request.target.decode("ascii"))
    method = request.method.decode("ascii")

    handler = router(target.path, method)

    body = bytearray()
    while True:
        event = await wrapper.next_event()
        if type(event) is h11.EndOfMessage:
            break
        assert type(event) is h11.Data
        body += event.data

    # headers = tuple(
    #     (name.decode("ascii"), value.decode("ascii"))
    #     for (name, value) in request.headers
    # )
    if body:
        request_body = json.loads(body.decode("ascii"))
    else:
        request_body = dict(parse_qsl(target.query))
    try:
        response = await handler(context, request_body)
    except Exception as e:
        wrapper.info("Exception during handler processing of", method, target.path, ":", str(e))
        response = {"error": str(e)}
    response_body_unicode = json.dumps(response)
    response_body_bytes = response_body_unicode.encode("utf-8")
    await send_simple_response(
        wrapper, 200, "application/json; charset=utf-8", response_body_bytes
    )


class JSONHTTPServer(Generic[TContext]):
    logger = logging.getLogger("trinity._utils.trio_utils.JSONHTTPServer")

    def __init__(self, handlers: Handlers[TContext], context: TContext, port: int = 0):
        router = make_router(handlers)
        self.context = context
        self.handler = http_serve_json_api(router, context)
        self.port = port

    async def serve(
        self, task_status: TaskStatus[int] = trio.TASK_STATUS_IGNORED
    ) -> None:
        async with trio.open_nursery() as nursery:
            # NOTE: `mypy` does not like the typing here but this
            # should type check...
            listeners: Sequence[trio.SocketListener] = await nursery.start(
                trio.serve_tcp, self.handler, self.port  # type: ignore
            )
            self.port = int(listeners[0].socket.getsockname()[1])
            task_status.started(self.port)
