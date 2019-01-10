from typing import (
    NamedTuple,
)

from rlp import sedes

from p2p.protocol import (
    Command,
)


class BroadcastDataMessage(NamedTuple):
    data: bytes


class BroadcastData(Command):
    _cmd_id = 0
    message_class = BroadcastDataMessage
    structure = [
        ('data', sedes.binary),
    ]


class GetSumMessage(NamedTuple):
    a: int
    b: int


class GetSum(Command):
    _cmd_id = 2
    message_class = GetSumMessage
    structure = [
        ('a', sedes.big_endian_int),
        ('b', sedes.big_endian_int),
    ]


class SumMessage(NamedTuple):
    result: int


class Sum(Command):
    _cmd_id = 3
    message_class = SumMessage
    structure = [
        ('result', sedes.big_endian_int),
    ]
