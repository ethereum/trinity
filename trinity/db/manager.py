from contextlib import contextmanager
import time
import logging
import socket
import pathlib
import threading


def _block_for_path(path):
    while not path.exists():
        time.sleep(0.001)


class DBManager:
    logger = logging.getLogger('manager')

    def __init__(self, db):
        self._started = threading.Event()
        self._stopped = threading.Event()
        self.db = db

    def is_started(self) -> bool:
        return self._stopped.is_set()

    def is_running(self) -> bool:
        return self.is_started and self.is_stopped

    def is_stopped(self) -> bool:
        return self._stopped.is_set()

    def wait_started(self) -> None:
        self._started.wait()

    def wait_stopped(self) -> None:
        self._stopped.wait()

    def start(self, ipc_path: pathlib.Path) -> None:
        threading.Thread(
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
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            # background task to close the socket.
            threading.Thread(
                target=self._close_socket_on_stop,
                args=(sock,),
                daemon=False,
            ).start()

            sock.bind(str(ipc_path))
            sock.listen(1)

            _block_for_path(ipc_path)
            self._started.set()

            while self.is_running:
                try:
                    conn, addr = sock.accept()
                except ConnectionAbortedError:
                    self._stopped.set()
                    return
                self.logger.debug('Server accepted connection from %s', addr)
                threading.Thread(
                    target=self._serve_conn,
                    args=(conn,),
                    daemon=False,
                ).start()

                self.logger.debug("%s: server stopping", self)

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
                self.logger.debug("closing connection, no operation %s", error)
                return

            if operation == b'\x00':
                # self.logger.debug("GET")
                key_length_data = read_exactly(4)
                key_length = int.from_bytes(key_length_data, 'little')
                key = read_exactly(key_length)
                try:
                    value = self.db[key]
                    value_length = len(value)
                    sock.sendall(b'\x01' + value_length.to_bytes(4, 'little') + value)
                except KeyError:
                    # self.logger.debug("Key %s doesn't exist", key)
                    sock.sendall(b'\x00')
            elif operation == b'\x01':
                # self.logger.debug("SET")
                length_data = read_exactly(8)
                key_length = int.from_bytes(length_data[:4], 'little')
                value_length = int.from_bytes(length_data[4:], 'little')
                payload = read_exactly(key_length + value_length)
                key, value = payload[:key_length], payload[key_length:]
                self.db[key] = value
                sock.sendall((1).to_bytes(1, 'little'))
            elif operation == b'\x02':
                # self.logger.debug("DEL")
                key_length_data = read_exactly(4)
                key_length = int.from_bytes(key_length_data, 'little')
                key = read_exactly(key_length)
                del self.db[key]
                sock.sendall(b'\x00')
            elif operation == b'\x03':
                # self.logger.debug("EXIST")
                key_length_data = read_exactly(4)
                key_length = int.from_bytes(key_length_data, 'little')
                key = read_exactly(key_length)
                result = key in self.db
                # self.logger.debug("Existance of %s, %s", key, result)
                sock.sendall(result.to_bytes(1, 'little'))
            else:
                raise Exception(f"Got unknown operation {operation}")

        assert False

    @contextmanager
    def run(self, ipc_path):
        self.start(ipc_path)
        try:
            yield self
        finally:
            self.stop()
