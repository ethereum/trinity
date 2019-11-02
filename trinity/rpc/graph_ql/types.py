import os
from typing import (
    Union,
    Dict,
    Any
)

from eth_utils import (
    encode_hex,
    int_to_big_endian,
    to_bytes,
    is_address,
    to_wei,
)

from graphql.language.ast import (
   StringValue,
)
from graphene import (
    Argument,
    InputObjectType,
    ObjectType,
    String,
    Field,
    Int,
    Schema,
    Scalar,
    List,
)

from trinity._utils.validation import (
    validate_transaction_gas_estimation_dict,
    validate_transaction_call_dict
)
from trinity.rpc.format import merge_transaction_defaults, to_int_if_hex
from trinity.rpc.utils import (
    state_at_block,
    get_header,
    dict_to_spoof_transaction,
)


class BaseBytes(Scalar):
    @staticmethod
    def parse_value(value):
        return to_bytes(hexstr=value)

    @staticmethod
    def serialize(value):
        return encode_hex(value)


class Address(BaseBytes):

    @staticmethod
    def parse_literal(node):
        if isinstance(node, StringValue) and is_address(node.value):
            return to_bytes(hexstr=node.value)


class Bytes(BaseBytes):

    @staticmethod
    def parse_literal(node):
        if isinstance(node, StringValue):
            return to_bytes(hexstr=node.value)


class Bytes32(BaseBytes):

    @staticmethod
    def parse_literal(node):
        if isinstance(node, StringValue):
            parsed_value = to_bytes(hexstr=node.value)
            if len(parsed_value) == 32:
                return parsed_value


class Account(ObjectType):
    address = Address()
    balance = Int()
    code = Bytes()
    storage = Bytes32(slot=Bytes(required=True))
    transactionCount = String()

    async def resolve_address(self, info):
        return self

    async def resolve_balance(self, info):
        at_block = info.context.get('at_block', 'latest')
        chain = info.context.get('chain')
        state = await state_at_block(chain, at_block)
        return state.get_balance(self)

    async def resolve_code(self, info):
        at_block = info.context.get('at_block', 'latest')
        chain = info.context.get('chain')
        state = await state_at_block(chain, at_block)
        return state.get_code(self)

    async def resolve_storage(self, info, slot):
        at_block = info.context.get('at_block', 'latest')
        chain = info.context.get('chain')
        state = await state_at_block(chain, at_block)
        position = to_int_if_hex(encode_hex(slot))
        return int_to_big_endian(state.get_storage(self, position))

    async def resolve_transactionCount(self, info):
        at_block = info.context.get('at_block', 'latest')
        chain = info.context.get('chain')
        state = await state_at_block(chain, at_block)
        nonce = state.get_nonce(self)
        return hex(nonce)


class Transaction(ObjectType):
    hash = String()
    nonce = String()
    value = String()
    index = String()
    sender = Field(Account, name='from')
    to = Field(Account)

    async def resolve_hash(self, info):
        return encode_hex(self.hash)

    async def resolve_nonce(self, info):
        return hex(self.nonce)

    async def resolve_value(self, info):
        return hex(self.value)

    async def resolve_index(self, info):
        # FIXME: Figure out a way to get this value
        return None

    async def resolve_sender(self, info):
        return self.sender

    async def resolve_to(self, info):
        return self.to


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
    transactions = List(Transaction)
    transactionCount = String()
    transactionAt = Field(Transaction, index=Int())

    def resolve_number(self, info):
        return hex(self.number)

    def resolve_hash(self, info):
        return encode_hex(self.header.hash)

    async def resolve_parent(self, info):
        chain = info.context.get('chain')
        parent_hash = self.header.parent_hash
        return await chain.coro_get_block_by_hash(parent_hash)

    def resolve_nonce(self, info):
        return hex(self.header.nonce)

    def resolve_transactionsRoot(self, info):
        return encode_hex(self.header.transaction_root)

    def resolve_stateRoot(self, info):
        return encode_hex(self.header.state_root)

    def resolve_receiptsRoot(self, info):
        return encode_hex(self.header.receipt_root)

    def resolve_miner(self, info):
        return encode_hex(self.header.coinbase)

    def resolve_extraData(self, info):
        return encode_hex(self.header.extra_data)

    def resolve_gasUsed(self, info):
        return hex(self.header.gas_used)

    def resolve_gasLimit(self, info):
        return hex(self.header.gas_limit)

    def resolve_timestamp(self, info):
        return hex(self.header.timestamp)

    def resolve_logsBloom(self, info):
        logs_bloom = encode_hex(int_to_big_endian(self.header.bloom))[2:]
        logs_bloom = '0x' + logs_bloom.rjust(512, '0')
        return logs_bloom

    def resolve_mixHash(self, info):
        return hex(self.header.mix_hash)

    def resolve_difficulty(self, info):
        return hex(self.header.difficulty)

    def resolve_totalDifficulty(self, info):
        chain = info.context.get('chain')
        return hex(chain.get_score(self.hash))

    def resolve_transactions(self, info):
        return self.transactions

    def resolve_transactionCount(self, info):
        return hex(len(self.transactions))

    def resolve_transactionAt(self, info: Dict[Any, Any], index: int) -> Transaction:
        return self.transactions[index]


class CallData(InputObjectType):
    sender = Address(name='from')
    to = Address()
    gas = Int()
    gasPrice = Int()
    value = String()
    data = Bytes()


class CallResult(ObjectType):
    data = Bytes()
    gas_used = Int(name='gasUsed')
    status = Int()

    def resolve_data(self, info):
        return self.output

    def resolve_gas_used(self, info):
        return self.get_gas_used()

    def resolve_status(self, info):
        return 0 if self.is_error else 1


class Query(ObjectType):
    block = Field(Block, number=Int(), hash=String())
    transaction = Field(Transaction, hash=String())
    estimate_gas = Int(
        data=Argument(CallData, required=True),
        at_block=String(name='blockNumber', required=True),
        name='estimateGas'
    )
    gas_price = Int(name='gasPrice')
    account = Field(
        Account,
        address=Address(required=True),
        at_block=String(name='blockNumber')
    )
    call = Field(
        CallResult,
        data=Argument(CallData, required=True),
        at_block=String(name='blockNumber')
    )

    async def resolve_block(self, info, number=None, hash=None):
        chain = info.context.get('chain')
        if number and hash:
            # TODO: change this execption type
            raise Exception('either pass number or hash')
        if number:
            result = await chain.coro_get_canonical_block_by_number(number)
            return result
        elif hash:
            return await chain.coro_get_block_by_hash(to_bytes(hexstr=hash))
        else:
            return await chain.coro_get_canonical_block_by_number(
                chain.get_canonical_head().block_number
            )

    async def resolve_transaction(self, info, hash):
        chain = info.context.get('chain')
        return chain.get_canonical_transaction(to_bytes(hexstr=hash))

    async def resolve_estimate_gas(self, info, data, at_block: Union[str, int]):
        chain = info.context.get('chain')
        header = await get_header(chain, at_block)
        validate_transaction_gas_estimation_dict(data, chain.get_vm(header))
        transaction = dict_to_spoof_transaction(
            chain,
            header,
            data,
            normalize_transaction=merge_transaction_defaults
        )
        gas = chain.estimate_gas(transaction, header)
        return gas

    async def resolve_gas_price(self, info):
        return int(os.environ.get('TRINITY_GAS_PRICE', to_wei(1, 'gwei')))

    async def resolve_account(self, info, address, at_block='latest'):
        info.context['at_block'] = at_block
        return address

    async def resolve_call(self, info, data, at_block='latest'):
        chain = info.context.get('chain')
        header = await get_header(chain, at_block)
        validate_transaction_call_dict(data, chain.get_vm(header))
        transaction = dict_to_spoof_transaction(
            chain,
            header,
            data,
            normalize_transaction=merge_transaction_defaults
        )
        computation = chain.get_transaction_computation(transaction, header)
        return computation


schema = Schema(query=Query)
