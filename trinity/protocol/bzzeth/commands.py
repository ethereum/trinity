from rlp import sedes

from eth.rlp.headers import BlockHeader

from p2p.protocol import (
    Command,
)


hash_sedes = sedes.Binary(min_length=32, max_length=32)


class Handshake(Command):
    _cmd_id = 0
    structure = (
        ('serves_headers', sedes.big_endian_int),
    )


class NewBlockHeaders(Command):
    _cmd_id = 1
    structure = sedes.CountableList(sedes.List([hash_sedes, sedes.big_endian_int]))


class GetBlockHeaders(Command):
    _cmd_id = 2
    structure = (
        ('request_id', sedes.big_endian_int),
        ('hashes', sedes.CountableList(hash_sedes)),
    )


class BlockHeaders(Command):
    _cmd_id = 3
    structure = (
        ('request_id', sedes.big_endian_int),
        ('headers', sedes.CountableList(BlockHeader)),
    )
