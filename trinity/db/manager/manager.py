from contextlib import (
    contextmanager,
)
import logging
import pathlib
import socket
import threading

from trinity._utils.ipc import (
    wait_for_ipc,
)

from .schema import (
    DELETE,
    EXIST,
    GET,
    SET,
)
from .utils import (
    bytes_to_int,
)


class DBManager:
    logger = logging.getLogger('manager')

    def __init__(self, db):
        self._started = threading.Event()
        self._stopped = threading.Event()
        self.db = db

    @property
    def is_started(self) -> bool:
        return self._started.is_set()

    @property
    def is_running(self) -> bool:
        return self.is_started and not self.is_stopped

    @property
    def is_stopped(self) -> bool:
        return self._stopped.is_set()

    def wait_started(self) -> None:
        self._started.wait()

    def wait_stopped(self) -> None:
        self._stopped.wait()

    def start(self, ipc_path: pathlib.Path) -> None:
        threading.Thread(
            name="serve",
            target=self.serve,
            args=(ipc_path,),
            daemon=False,
        ).start()
        self.wait_started()

    def stop(self) -> None:
        self._stopped.set()

    def _close_socket_on_stop(self, sock: socket.socket) -> None:
        self.wait_stopped()
        sock.close()

    def serve(self, ipc_path: pathlib.Path) -> None:
        self.logger.info("server connect")

        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            # background task to close the socket.
            threading.Thread(
                name="_close_socket_on_stop",
                target=self._close_socket_on_stop,
                args=(sock,),
                daemon=False,
            ).start()

            sock.bind(str(ipc_path))
            sock.listen(1)

            wait_for_ipc(ipc_path)
            self._started.set()

            while self.is_running:
                try:
                    conn, addr = sock.accept()
                except ConnectionAbortedError:
                    self._stopped.set()
                    return
                self.logger.debug('Server accepted connection from %s', addr)
                threading.Thread(
                    name="_serve_conn",
                    target=self._serve_conn,
                    args=(conn,),
                    daemon=False,
                ).start()

    def _serve_conn(self, sock: socket.socket) -> None:
        self.logger.debug("%s: starting client handler for %s", self, sock)
        buffer = bytearray()

        def read_exactly(num_bytes):
            nonlocal buffer
            while len(buffer) < num_bytes:

                data = sock.recv(4096)

                if data == b"":
                    raise Exception("Connection closed")

                buffer += data
            payload = buffer[:num_bytes]
            buffer = buffer[num_bytes:]
            return bytes(payload)

        while self.is_running:
            try:
                operation = read_exactly(1)
            except Exception as error:
                self.logger.info("closing connection, no operation %s", error)
                return
            self.logger.info("Server listening here, got operation %s", operation)
            if operation == GET.code:
                key = GET.server_reads_client_request(read_exactly)
                try:
                    value = self.db[key]
                    sock.sendall(GET.server_responds_success_message(value))
                except KeyError:
                    sock.sendall(GET.server_responds_fail_message())
            elif operation == SET.code:
                key, value = SET.server_reads_client_request(read_exactly)
                try:
                    self.db[key] = value
                    sock.sendall(SET.server_responds_success_message())
                except Exception as error:
                    sock.sendall(SET.server_responds_fail_message())
            elif operation == DELETE.code:
                key = DELETE.server_reads_client_request(read_exactly)
                try:
                    del self.db[key]
                    sock.sendall(DELETE.server_responds_success_message())
                except Exception as error:
                    sock.sendall(DELETE.server_responds_fail_message())
            elif operation == EXIST.code:
                key = EXIST.server_reads_client_request(read_exactly)
                try:
                    result = key in self.db
                    sock.sendall(EXIST.server_responds_success_message(result))
                except Exception as error:
                    sock.sendall(EXIST.server_responds_fail_message())
            else:
                raise Exception(f"Got unknown operation {operation}")

    @contextmanager
    def run(self, ipc_path):
        self.start(ipc_path)
        try:
            yield self
        finally:
            self.stop()
