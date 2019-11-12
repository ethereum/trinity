import socket


class BufferedSocket:
    def __init__(self, sock: socket.socket) -> None:
        self._socket = sock
        self._buffer = bytearray()
        self.sendall = sock.sendall
        self.close = sock.close
        self.shutdown = sock.shutdown

    def read_exactly(self, num_bytes: int) -> bytes:
        while len(self._buffer) < num_bytes:

            data = self._socket.recv(4096)

            if data == b"":
                raise OSError("Connection closed")

            self._buffer.extend(data)
        payload = self._buffer[:num_bytes]
        self._buffer = self._buffer[num_bytes:]
        return bytes(payload)
