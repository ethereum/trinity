import os

from typing import (
    Any,
    AnyStr,
    Dict,
    List as TList,
    Optional,
    Union,
)

from eth.abc import ComputationAPI
from eth.rlp.blocks import BaseBlock
from eth.rlp.transactions import BaseTransaction
from eth_typing import (
    Address as TAddress,
    HexStr,
    BlockNumber
)
from eth_utils import (
    encode_hex,
    int_to_big_endian,
    to_bytes,
    is_address,
    to_wei,
    ValidationError,
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
from graphql.language.ast import (
    Node,
    StringValue,
)
from graphql.execution.base import ResolveInfo
from trinity._utils.validation import (
    validate_transaction_gas_estimation_dict,
    validate_transaction_call_dict
)
from trinity.rpc.format import (
    merge_transaction_defaults,
    to_int_if_hex,
)
from trinity.rpc.utils import (
    state_at_block,
    get_header,
    dict_to_spoof_transaction,
)


class BaseBytes(Scalar):
    @staticmethod
    def parse_value(value: HexStr) -> bytes:
        return to_bytes(hexstr=value)

    @staticmethod
    def serialize(value: AnyStr) -> str:
        return encode_hex(value)


class Address(BaseBytes):

    @staticmethod
    def parse_literal(node: Node) -> Optional[bytes]:
        if isinstance(node, StringValue) and is_address(node.value):
            return to_bytes(hexstr=node.value)
        return None


class Bytes(BaseBytes):

    @staticmethod
    def parse_literal(node: Node) -> Optional[bytes]:
        if isinstance(node, StringValue):
            return to_bytes(hexstr=node.value)
        return None


class Bytes32(BaseBytes):

    @staticmethod
    def parse_literal(node: Node) -> Optional[bytes]:
        if isinstance(node, StringValue):
            parsed_value = to_bytes(hexstr=node.value)
            if len(parsed_value) == 32:
                return parsed_value
        return None


class Account(ObjectType):
    address = Address()
    balance = Int()
    code = Bytes()
    storage = Bytes32(slot=Int(required=True))
    transactionCount = String()

    @staticmethod
    async def resolve_address(address: TAddress, info: ResolveInfo) -> TAddress:
        return address

    @staticmethod
    async def resolve_balance(address: TAddress, info: ResolveInfo) -> int:
        at_block = info.context.get('at_block', 'latest')
        chain = info.context.get('chain')
        state = await state_at_block(chain, at_block)
        return state.get_balance(address)

    @staticmethod
    async def resolve_code(address: TAddress, info: ResolveInfo) -> bytes:
        at_block = info.context.get('at_block', 'latest')
        chain = info.context.get('chain')
        state = await state_at_block(chain, at_block)
        return state.get_code(address)

    @staticmethod
    async def resolve_storage(address: TAddress, info: ResolveInfo, slot: int) -> int:
        at_block = info.context.get('at_block', 'latest')
        chain = info.context.get('chain')
        state = await state_at_block(chain, at_block)
        position = to_int_if_hex(slot)
        return int_to_big_endian(state.get_storage(address, position))  # type: ignore

    @staticmethod
    async def resolve_transactionCount(address: TAddress, info: ResolveInfo) -> str:
        at_block = info.context.get('at_block', 'latest')
        chain = info.context.get('chain')
        state = await state_at_block(chain, at_block)
        nonce = state.get_nonce(address)
        return hex(nonce)


class Transaction(ObjectType):
    hash = String()
    nonce = String()
    value = String()
    index = String()
    sender = Field(Account, name='from')
    to = Field(Account)

    @staticmethod
    async def resolve_hash(transaction: BaseTransaction, info: ResolveInfo) -> str:
        return encode_hex(transaction.hash)

    @staticmethod
    async def resolve_nonce(transaction: BaseTransaction, info: ResolveInfo) -> str:
        return hex(transaction.nonce)

    @staticmethod
    async def resolve_value(transaction: BaseTransaction, info: ResolveInfo) -> str:
        return hex(transaction.value)

    @staticmethod
    async def resolve_index(transaction: BaseTransaction, info: ResolveInfo) -> None:
        # FIXME: Figure out a way to get this value
        return None

    @staticmethod
    async def resolve_sender(transaction: BaseTransaction, info: ResolveInfo) -> TAddress:
        return transaction.sender

    @staticmethod
    async def resolve_to(transaction: BaseTransaction, info: ResolveInfo) -> TAddress:
        return transaction.to


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

    @staticmethod
    def resolve_number(block: BaseBlock, info: ResolveInfo) -> str:
        return hex(block.number)

    @staticmethod
    def resolve_hash(block: BaseBlock, info: ResolveInfo) -> str:
        return encode_hex(block.header.hash)

    @staticmethod
    async def resolve_parent(block: BaseBlock, info: ResolveInfo) -> BaseBlock:
        chain = info.context.get('chain')
        parent_hash = block.header.parent_hash
        return await chain.coro_get_block_by_hash(parent_hash)

    @staticmethod
    def resolve_nonce(block: BaseBlock, info: ResolveInfo) -> str:
        return hex(block.header.nonce)

    @staticmethod
    def resolve_transactionsRoot(block: BaseBlock, info: ResolveInfo) -> str:
        return encode_hex(block.header.transaction_root)

    @staticmethod
    def resolve_stateRoot(block: BaseBlock, info: ResolveInfo) -> str:
        return encode_hex(block.header.state_root)

    @staticmethod
    def resolve_receiptsRoot(block: BaseBlock, info: ResolveInfo) -> str:
        return encode_hex(block.header.receipt_root)

    @staticmethod
    def resolve_miner(block: BaseBlock, info: ResolveInfo) -> str:
        return encode_hex(block.header.coinbase)

    @staticmethod
    def resolve_extraData(block: BaseBlock, info: ResolveInfo) -> str:
        return encode_hex(block.header.extra_data)

    @staticmethod
    def resolve_gasUsed(block: BaseBlock, info: ResolveInfo) -> str:
        return hex(block.header.gas_used)

    @staticmethod
    def resolve_gasLimit(block: BaseBlock, info: ResolveInfo) -> str:
        return hex(block.header.gas_limit)

    @staticmethod
    def resolve_timestamp(block: BaseBlock, info: ResolveInfo) -> str:
        return hex(block.header.timestamp)

    @staticmethod
    def resolve_logsBloom(block: BaseBlock, info: ResolveInfo) -> str:
        logs_bloom = encode_hex(int_to_big_endian(block.header.bloom))[2:]
        logs_bloom = '0x' + logs_bloom.rjust(512, '0')
        return logs_bloom

    @staticmethod
    def resolve_mixHash(block: BaseBlock, info: ResolveInfo) -> str:
        return hex(block.header.mix_hash)

    @staticmethod
    def resolve_difficulty(block: BaseBlock, info: ResolveInfo) -> str:
        return hex(block.header.difficulty)

    @staticmethod
    def resolve_totalDifficulty(block: BaseBlock, info: ResolveInfo) -> str:
        chain = info.context.get('chain')
        return hex(chain.get_score(block.hash))

    @staticmethod
    def resolve_transactions(block: BaseBlock, info: ResolveInfo) -> TList[BaseTransaction]:
        return block.transactions

    @staticmethod
    def resolve_transactionCount(block: BaseBlock, info: ResolveInfo) -> str:
        return hex(len(block.transactions))

    @staticmethod
    def resolve_transactionAt(
            block: BaseBlock, info: Dict[Any, Any], index: int
    ) -> BaseTransaction:
        return block.transactions[index]


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

    @staticmethod
    def resolve_data(computation: ComputationAPI, info: ResolveInfo) -> bytes:
        return computation.output

    @staticmethod
    def resolve_gas_used(computation: ComputationAPI, info: ResolveInfo) -> int:
        return computation.get_gas_used()

    @staticmethod
    def resolve_status(computation: ComputationAPI, info: ResolveInfo) -> int:
        return 0 if computation.is_error else 1


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

    @staticmethod
    async def resolve_block(
            _: Any, info: ResolveInfo, number: Optional[int]=None, hash: Optional[HexStr]=None
    ) -> Union[BaseBlock, BlockNumber]:
        chain = info.context.get('chain')
        if number and hash:
            raise ValidationError('passing number and hash together is not allowed')
        if number:
            result = await chain.coro_get_canonical_block_by_number(number)
            return result
        elif hash:
            return await chain.coro_get_block_by_hash(to_bytes(hexstr=hash))
        else:
            return await chain.coro_get_canonical_block_by_number(
                chain.get_canonical_head().block_number
            )

    @staticmethod
    async def resolve_transaction(_: Any, info: ResolveInfo, hash: HexStr) -> BaseTransaction:
        chain = info.context.get('chain')
        return chain.get_canonical_transaction(to_bytes(hexstr=hash))

    @staticmethod
    async def resolve_estimate_gas(
            _: Any, info: ResolveInfo, data: Dict[str, Any], at_block: Union[str, int]
    ) -> int:
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

    @staticmethod
    async def resolve_gas_price(_: Any, info: ResolveInfo) -> int:
        return int(os.environ.get('TRINITY_GAS_PRICE', to_wei(1, 'gwei')))

    @staticmethod
    async def resolve_account(
            _: Any, info: ResolveInfo, address: HexStr, at_block: Union[str, int]='latest'
    ) -> HexStr:
        info.context['at_block'] = at_block
        return address

    @staticmethod
    async def resolve_call(
            _: Any,
            info: ResolveInfo,
            data: Dict[str, AnyStr],
            at_block: Union[str, int]='latest'
    ) -> ComputationAPI:
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
