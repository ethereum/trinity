from rlp import sedes

from eth.rlp.headers import BlockHeader
from eth.rlp.receipts import Receipt
from eth.rlp.transactions import BaseTransactionFields

from trinity.protocol.common.commands import SnappyCommand
from trinity.rlp.block_body import BlockBody
from trinity.rlp.sedes import HashOrNumber


hash_sedes = sedes.Binary(min_length=32, max_length=32)


class Status(SnappyCommand):
    _cmd_id = 0
    structure = (
        ('protocol_version', sedes.big_endian_int),
        ('network_id', sedes.big_endian_int),
        ('best_hash', hash_sedes),
        ('genesis_hash', hash_sedes),
    )


class NewBlockWitnessHashes(SnappyCommand):
    _cmd_id = 1
    structure = (
        ('block_hash', hash_sedes),
        ('node_hashes', sedes.CountableList(hash_sedes)),
    )
