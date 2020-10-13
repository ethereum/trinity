import socket


def get_open_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", 0))
    s.listen(1)
    port = s.getsockname()[1]
    s.close()
    # type ignored to fix https://github.com/ethereum/trinity/issues/1520
    return port  # type: ignore
