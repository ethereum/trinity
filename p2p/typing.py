from typing import Any, Tuple, TypeVar, Union, NewType, NamedTuple

TCommandPayload = TypeVar('TCommandPayload')


Structure = Union[
    Tuple[Tuple[str, Any], ...],
]


Capability = Tuple[str, int]
Capabilities = Tuple[Capability, ...]
AES128Key = NewType("AES128Key", bytes)
Nonce = NewType("Nonce", bytes)
IDNonce = NewType("IDNonce", bytes)


class SessionKeys(NamedTuple):
    encryption_key: AES128Key
    decryption_key: AES128Key
    auth_response_key: AES128Key
