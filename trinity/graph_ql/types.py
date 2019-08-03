from eth_utils import (
    encode_hex,
    int_to_big_endian,
)
from graphene import (
    ObjectType,
    String,
    Field,
    Int,
    Schema,
)


class Block(ObjectType):
    number = String()
    hash = String()
    parent = Field(lambda: Block)
    nonce = String()
    transactionsRoot = String()
    stateRoot = String()
    receiptsRoot = String()
    miner = String()
    extraData = String()
    gasLimit = String()
    gasUsed = String()
    timestamp = String()
    logsBloom = String()
    mixHash = String()
    difficulty = String()
    totalDifficulty = String()

    async def resolve_number(self, info):
        return hex(self.number)  # type: ignore

    async def resolve_hash(self, info):
        return encode_hex(self.header.hash)

    async def resolve_parent(self, info):
        chain = info.context.get('chain')
        parent_hash = self.header.parent_hash
        return await chain.coro_get_block_by_hash(parent_hash)

    async def resolve_nonce(self, info):
        return hex(self.header.nonce)

    async def resolve_transactionsRoot(self, info):
        return encode_hex(self.header.transaction_root)

    async def resolve_stateRoot(self, info):
        return encode_hex(self.header.state_root)

    async def resolve_receiptsRoot(self, info):
        return encode_hex(self.header.receipt_root)

    async def resolve_miner(self, info):
        return encode_hex(self.header.coinbase)

    async def resolve_extraData(self, info):
        return encode_hex(self.header.extra_data)

    async def resolve_gasUsed(self, info):
        return hex(self.header.gas_used)

    async def resolve_gasLimit(self, info):
        return hex(self.header.gas_limit)

    async def resolve_timestamp(self, info):
        return hex(self.header.timestamp)

    async def resolve_logsBloom(self, info):
        logs_bloom = encode_hex(int_to_big_endian(self.header.bloom))[2:]
        logs_bloom = '0x' + logs_bloom.rjust(512, '0')
        return logs_bloom

    async def resolve_mixHash(self, info):
        return hex(self.header.mix_hash)

    async def resolve_difficulty(self, info):
        return hex(self.header.difficulty)

    async def resolve_difficulty(self, info):
        chain = info.context.get('chain')
        return hex(chain.get_score(self.hash))
import logging
logger = logging.getLogger("GraphQlServer")
class Query(ObjectType):
    block = Field(Block, number=Int(), hash=String())

    async def resolve_block(self, info, number=None, hash=None):
        logger.info(f'inisde resolve_block')
        chain = info.context.get('chain')
        if number and hash:
            raise Exception('either pass number or hash')
        if number:
            result = await chain.coro_get_canonical_block_by_number(number)
            logger.info(f'resolve_block result {result}')
            return result
        elif hash:
            return await chain.coro_get_block_by_hash(hash)
        else:
            return await chain.coro_get_canonical_block_by_number(
                chain.get_canonical_head().block_number
            )


schema = Schema(query=Query)
