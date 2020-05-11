from eth_utils import encode_hex, decode_hex

from werkzeug.routing import BaseConverter


class HexadecimalPublicKeyConverter(BaseConverter):
    regex = r"0x?([0-9a-fA-F]{96})"

    def to_python(self, value: str) -> bytes:
        return decode_hex(value)

    def to_url(self, value: bytes):
        return encode_hex(value)
