import rlp

from eth_hash.auto import keccak
from eth_typing import Address


def force_bytes_to_address(value: bytes) -> Address:
    """
    Take any byte value and force it into becoming a valid address by padding it to 20 bytes
    or returning the first 20 bytes in case the provided value is longer than 20 bytes.
    """
    trimmed_value = value[-20:]
    padded_value = trimmed_value.rjust(20, b'\x00')
    return Address(padded_value)


def generate_contract_address(sender: Address, nonce: int) -> Address:
    """
    Take the given ``sender```and ``nonce`` and generate an address in the same way
    as a *Contract Creation Transaction* creates a contract address when a contract
    is deployed.
    """
    return force_bytes_to_address(keccak(rlp.encode([sender, nonce])))
