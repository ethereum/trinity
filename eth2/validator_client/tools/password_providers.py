import getpass

from eth_typing import BLSPubkey
from eth_utils import encode_hex


def terminal_password_provider(public_key: BLSPubkey) -> bytes:
    return getpass.getpass(
        f"Please enter password for keyfile with public key {encode_hex(public_key)}:"
    ).encode()
