from typing import (
    cast,
    NamedTuple,
    Tuple,
    Union,
)

import rlp
from rlp.sedes import (
    big_endian_int,
)
from rlp.exceptions import (
    DecodingError,
)

from eth_utils import (
    encode_hex,
    is_bytes,
    is_list_like,
    ValidationError,
)
from eth_typing import (
    Hash32,
)

from eth.validation import (
    validate_length,
    validate_length_lte,
)

from p2p.discv5.encryption import (
    validate_nonce,
    Nonce,
)
from p2p.discv5.constants import (
    AUTH_SCHEME_NAME,
    MAX_PACKET_SIZE,
    TAG_SIZE,
    MAGIC_SIZE,
)


#
# Packet data structures
#
class WhoAreYouPacket(NamedTuple):
    tag: Hash32
    magic: Hash32
    token: Nonce
    id_nonce: bytes
    enr_sequence_number: int


class AuthHeader(NamedTuple):
    auth_tag: Nonce
    auth_scheme_name: bytes
    ephemeral_pubkey: bytes
    encrypted_auth_response: bytes


class AuthHeaderPacket(NamedTuple):
    tag: Hash32
    auth_header: AuthHeader
    encrypted_message: bytes


class AuthTagPacket(NamedTuple):
    tag: Hash32
    auth_tag: Nonce
    encrypted_message: bytes


#
# Validation
#
def validate_who_are_you_packet_size(encoded_packet: bytes) -> None:
    validate_max_packet_size(encoded_packet)
    validate_tag_prefix(encoded_packet)
    if len(encoded_packet) - TAG_SIZE < MAGIC_SIZE:
        raise ValidationError(
            f"Encoded packet is only {len(encoded_packet)} bytes, but should contain {MAGIC_SIZE} "
            f"bytes of magic following the {TAG_SIZE} tag at the beginning."
        )
    if len(encoded_packet) - TAG_SIZE - MAGIC_SIZE < 1:
        raise ValidationError(
            f"Encoded packet is missing RLP encoded payload section"
        )


def validate_message_packet_size(encoded_packet: bytes) -> None:
    validate_max_packet_size(encoded_packet)
    validate_tag_prefix(encoded_packet)
    if len(encoded_packet) - TAG_SIZE < 1:
        raise ValidationError(
            f"Message packet is missing RLP encoded auth section"
        )


def validate_max_packet_size(encoded_packet: bytes) -> None:
    validate_length_lte(encoded_packet, MAX_PACKET_SIZE, "packet")


def validate_tag_prefix(encoded_packet: bytes) -> None:
    if len(encoded_packet) < TAG_SIZE:
        raise ValidationError(
            f"Encoded packet is only {len(encoded_packet)} bytes, but should start with a "
            f"{TAG_SIZE} bytes tag"
        )


def validate_auth_header(auth_header: AuthHeader) -> None:
    validate_nonce(auth_header.auth_tag)
    if auth_header.auth_scheme_name != AUTH_SCHEME_NAME:
        raise ValidationError(
            f"Auth header uses scheme {auth_header.auth_scheme_name}, but only "
            f"{AUTH_SCHEME_NAME} is supported"
        )


#
# Packet encoding
#
def encode_who_are_you_packet(packet: WhoAreYouPacket) -> bytes:
    message = rlp.encode((
        packet.token,
        packet.id_nonce,
        packet.enr_sequence_number,
    ))

    encoded_packet = b"".join((
        packet.tag,
        packet.magic,
        message,
    ))

    validate_who_are_you_packet_size(encoded_packet)
    return encoded_packet


def encode_auth_header_packet(packet: AuthHeaderPacket) -> bytes:
    encoded_packet = b"".join((
        packet.tag,
        rlp.encode(packet.auth_header),
        packet.encrypted_message,
    ))
    validate_max_packet_size(encoded_packet)
    return encoded_packet


def encode_auth_tag_packet(packet: AuthTagPacket) -> bytes:
    encoded_packet = b"".join((
        packet.tag,
        rlp.encode(packet.auth_tag),
        packet.encrypted_message,
    ))
    validate_max_packet_size(encoded_packet)
    return encoded_packet


#
# Packet decoding
#
def decode_who_are_you_packet(encoded_packet: bytes) -> WhoAreYouPacket:
    validate_who_are_you_packet_size(encoded_packet)

    tag = _decode_tag(encoded_packet)
    magic = _decode_who_are_you_magic(encoded_packet)
    token, id_nonce, enr_seq = _decode_who_are_you_payload(encoded_packet)
    return WhoAreYouPacket(
        tag=tag,
        magic=magic,
        token=token,
        id_nonce=id_nonce,
        enr_sequence_number=enr_seq,
    )


def decode_message_packet(encoded_packet: bytes) -> Union[AuthTagPacket, AuthHeaderPacket]:
    validate_message_packet_size(encoded_packet)

    tag = _decode_tag(encoded_packet)
    auth, message_start_index = _decode_auth(encoded_packet)
    encrypted_message = encoded_packet[message_start_index:]

    packet: Union[AuthTagPacket, AuthHeaderPacket]
    if is_bytes(auth):
        packet = AuthTagPacket(
            tag=tag,
            auth_tag=cast(Nonce, auth),
            encrypted_message=encrypted_message,
        )
    elif isinstance(auth, AuthHeader):
        packet = AuthHeaderPacket(
            tag=tag,
            auth_header=auth,
            encrypted_message=encrypted_message,
        )
    else:
        raise Exception("Unreachable: decode_auth returns either Nonce or AuthHeader")

    return packet


def _decode_tag(encoded_packet: bytes) -> Hash32:
    return Hash32(encoded_packet[:TAG_SIZE])


def _decode_who_are_you_magic(encoded_packet: bytes) -> Hash32:
    return Hash32(encoded_packet[TAG_SIZE:TAG_SIZE + MAGIC_SIZE])


def _decode_who_are_you_payload(encoded_packet: bytes) -> Tuple[Nonce, bytes, int]:
    payload_rlp = encoded_packet[TAG_SIZE + MAGIC_SIZE:]

    try:
        payload = rlp.decode(payload_rlp)
    except DecodingError as error:
        raise ValidationError(
            f"WHOAREYOU payload section is not proper RLP: {encode_hex(payload_rlp)}"
        ) from error

    if not is_list_like(payload):
        raise ValidationError(
            f"WHOAREYOU payload section is not an RLP encoded list: {payload}"
        )
    if len(payload) != 3:
        raise ValidationError(
            f"WHOAREYOU payload consists of {len(payload)} instead of 3 elements: {payload}"
        )

    token, id_nonce, enr_seq_bytes = payload
    enr_seq = big_endian_int.deserialize(enr_seq_bytes)
    validate_nonce(token)
    return Nonce(token), id_nonce, enr_seq


def _decode_auth(encoded_packet: bytes) -> Tuple[Union[AuthHeader, Nonce], int]:
    try:
        decoded_auth, _, message_start_index = rlp.codec.consume_item(encoded_packet, TAG_SIZE)
    except DecodingError as error:
        raise ValidationError("Packet authentication section is not proper RLP") from error

    if is_bytes(decoded_auth):
        validate_nonce(decoded_auth)
        return Nonce(decoded_auth), message_start_index
    elif is_list_like(decoded_auth):
        validate_length(decoded_auth, 4, "auth header")
        for index, element in enumerate(decoded_auth):
            if not is_bytes(element):
                raise ValidationError(f"Element {index} in auth header is not bytes: {element}")
        auth_header = AuthHeader(
            auth_tag=decoded_auth[0],
            auth_scheme_name=decoded_auth[1],
            ephemeral_pubkey=decoded_auth[2],
            encrypted_auth_response=decoded_auth[3],
        )
        validate_auth_header(auth_header)
        return auth_header, message_start_index
    else:
        raise Exception("unreachable: RLP can only encode bytes and lists")
