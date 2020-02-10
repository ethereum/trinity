from eth_typing import BlockNumber
from eth_utils import to_bytes

from trinity.protocol.eth.forkid import ForkID

try:
    import factory
except ImportError:
    raise ImportError(
        "The p2p.tools.factories module requires the `factory_boy` and `faker` libraries."
    )

from typing import Any

from eth.abc import HeaderDatabaseAPI
from eth.constants import GENESIS_BLOCK_NUMBER

from trinity.constants import MAINNET_NETWORK_ID
from trinity.protocol.eth.payloads import (
    StatusV63Payload,
    NewBlockHash,
    NewBlockPayload,
    BlockFields,
    StatusPayload,
)
from trinity.protocol.eth.proto import ETHProtocolV63, ETHProtocol

from trinity.tools.factories.block_hash import BlockHashFactory
from trinity.tools.factories.headers import BlockHeaderFactory
from trinity.tools.factories.transactions import BaseTransactionFieldsFactory


class StatusV63PayloadFactory(factory.Factory):
    class Meta:
        model = StatusV63Payload

    version = ETHProtocolV63.version
    network_id = MAINNET_NETWORK_ID
    total_difficulty = 1
    head_hash = factory.SubFactory(BlockHashFactory)
    genesis_hash = factory.SubFactory(BlockHashFactory)

    @classmethod
    def from_headerdb(cls, headerdb: HeaderDatabaseAPI, **kwargs: Any) -> StatusV63Payload:
        head = headerdb.get_canonical_head()
        head_score = headerdb.get_score(head.hash)
        genesis = headerdb.get_canonical_block_header_by_number(GENESIS_BLOCK_NUMBER)
        return cls(
            head_hash=head.hash,
            genesis_hash=genesis.hash,
            total_difficulty=head_score,
            **kwargs
        )


class StatusPayloadFactory(factory.Factory):
    class Meta:
        model = StatusPayload

    version = ETHProtocol.version
    network_id = MAINNET_NETWORK_ID
    total_difficulty = 1
    head_hash = factory.SubFactory(BlockHashFactory)
    genesis_hash = factory.SubFactory(BlockHashFactory)
    fork_id = ForkID(to_bytes(hexstr='0xfc64ec04'), BlockNumber(1150000))  # unsynced

    @classmethod
    def from_headerdb(cls, headerdb: HeaderDatabaseAPI, **kwargs: Any) -> StatusPayload:
        head = headerdb.get_canonical_head()
        head_score = headerdb.get_score(head.hash)
        genesis = headerdb.get_canonical_block_header_by_number(GENESIS_BLOCK_NUMBER)
        return cls(
            head_hash=head.hash,
            genesis_hash=genesis.hash,
            total_difficulty=head_score,
            fork_id=cls.fork_id,
            **kwargs
        )


class NewBlockHashFactory(factory.Factory):
    class Meta:
        model = NewBlockHash

    hash = factory.SubFactory(BlockHashFactory)
    number = factory.Sequence(lambda n: n)


class BlockFieldsFactory(factory.Factory):
    class Meta:
        model = BlockFields

    header = factory.SubFactory(BlockHeaderFactory)
    transactions = factory.LazyFunction(lambda: tuple(BaseTransactionFieldsFactory.create_batch(2)))
    uncles = factory.LazyFunction(lambda: tuple(BlockHeaderFactory.create_batch(2)))


class NewBlockPayloadFactory(factory.Factory):
    class Meta:
        model = NewBlockPayload

    block = factory.SubFactory(BlockFieldsFactory)
    total_difficulty = 100
