import os

from eth_utils.toolz import (
    identity,
)
from typing import (
    Any,
    cast,
    Dict,
    List,
    NoReturn,
    Union,
)
from mypy_extensions import (
    TypedDict,
)

from eth_typing import (
    Address,
    BlockNumber,
    Hash32,
)
from eth_utils import (
    decode_hex,
    encode_hex,
    int_to_big_endian,
    is_integer,
    to_wei,
)

from eth.abc import (
    SignedTransactionAPI,
    StateAPI,
)
from eth.constants import (
    ZERO_ADDRESS,
)
from eth.exceptions import (
    HeaderNotFound,
    TransactionNotFound,
)
from eth.rlp.blocks import (
    BaseBlock,
)
from eth.rlp.headers import (
    BlockHeader,
)
from eth.vm.spoof import (
    SpoofTransaction,
)
from eth._utils.padding import (
    pad32,
)

from trinity.chains.base import AsyncChainAPI
from trinity.constants import (
    TO_NETWORKING_BROADCAST_CONFIG,
)
from trinity.exceptions import RpcError
from trinity.rpc.format import (
    block_to_dict,
    header_to_dict,
    format_params,
    normalize_transaction_dict,
    to_int_if_hex,
    to_receipt_response,
    transaction_to_dict,
)
from trinity.rpc.modules import (
    Eth1ChainRPCModule,
)
from trinity.rpc.retry import retryable
from trinity.rpc.typing import (
    RpcBlockResponse,
    RpcHeaderResponse,
    RpcReceiptResponse,
    RpcTransactionResponse,
)
from trinity.sync.common.events import (
    SyncingRequest,
)
from trinity._utils.validation import (
    validate_transaction_call_dict,
    validate_transaction_gas_estimation_dict,
)

from ._util import get_header


async def state_at_block(
        chain: AsyncChainAPI,
        at_block: Union[str, int],
        read_only: bool = True) -> StateAPI:
    at_header = await get_header(chain, at_block)
    vm = chain.get_vm(at_header)
    return vm.state


async def get_block_at_number(chain: AsyncChainAPI, at_block: Union[str, int]) -> BaseBlock:
    # mypy doesn't have user defined type guards yet
    # https://github.com/python/mypy/issues/5206
    if is_integer(at_block) and at_block >= 0:  # type: ignore
        # optimization to avoid requesting block, then header, then block again
        return await chain.coro_get_canonical_block_by_number(cast(BlockNumber, at_block))
    else:
        at_header = await get_header(chain, at_block)
        return await chain.coro_get_block_by_header(at_header)


def dict_to_spoof_transaction(
        chain: AsyncChainAPI,
        header: BlockHeader,
        transaction_dict: Dict[str, Any]) -> SignedTransactionAPI:
    """
    Convert dicts used in calls & gas estimates into a spoof transaction
    """
    txn_dict = normalize_transaction_dict(transaction_dict)
    sender = txn_dict.get('from', ZERO_ADDRESS)

    if 'nonce' in txn_dict:
        nonce = txn_dict['nonce']
    else:
        vm = chain.get_vm(header)
        nonce = vm.state.get_nonce(sender)

    gas_price = txn_dict.get('gasPrice', 0)
    gas = txn_dict.get('gas', header.gas_limit)

    unsigned = chain.get_vm_class(header).create_unsigned_transaction(
        nonce=nonce,
        gas_price=gas_price,
        gas=gas,
        to=txn_dict['to'],
        value=txn_dict['value'],
        data=txn_dict['data'],
    )
    return cast(SignedTransactionAPI, SpoofTransaction(unsigned, from_=sender))


class SyncProgressDict(TypedDict):
    startingBlock: BlockNumber
    currentBlock: BlockNumber
    highestBlock: BlockNumber


class Eth(Eth1ChainRPCModule):
    """
    All the methods defined by JSON-RPC API, starting with "eth_"...

    Any attribute without an underscore is publicly accessible.
    """
    async def accounts(self) -> List[str]:
        # trinity does not manage accounts for the user
        return []

    async def blockNumber(self) -> str:
        num = self.chain.get_canonical_head().block_number
        return hex(num)

    @retryable(which_block_arg_name='at_block')
    @format_params(identity, to_int_if_hex)
    async def call(self, txn_dict: Dict[str, Any], at_block: Union[str, int]) -> str:
        header = await get_header(self.chain, at_block)
        validate_transaction_call_dict(txn_dict, self.chain.get_vm(header))
        transaction = dict_to_spoof_transaction(self.chain, header, txn_dict)
        result = self.chain.get_transaction_result(transaction, header)
        return encode_hex(result)

    async def coinbase(self) -> str:
        raise NotImplementedError("Trinity does not support mining")

    @retryable(which_block_arg_name='at_block')
    @format_params(identity, to_int_if_hex)
    async def estimateGas(self, txn_dict: Dict[str, Any], at_block: Union[str, int]) -> str:
        header = await get_header(self.chain, at_block)
        validate_transaction_gas_estimation_dict(txn_dict, self.chain.get_vm(header))
        transaction = dict_to_spoof_transaction(self.chain, header, txn_dict)
        gas = self.chain.estimate_gas(transaction, header)
        return hex(gas)

    async def gasPrice(self) -> str:
        return hex(int(os.environ.get('TRINITY_GAS_PRICE', to_wei(1, 'gwei'))))

    @retryable(which_block_arg_name='at_block')
    @format_params(decode_hex, to_int_if_hex)
    async def getBalance(self, address: Address, at_block: Union[str, int]) -> str:
        state = await state_at_block(self.chain, at_block)
        balance = state.get_balance(address)

        return hex(balance)

    async def getWork(self) -> NoReturn:
        raise NotImplementedError("Trinity does not support mining")

    @format_params(decode_hex, decode_hex, decode_hex)
    async def submitWork(self, nonce: bytes, pow_hash: Hash32, mix_digest: Hash32) -> NoReturn:
        raise NotImplementedError("Trinity does not support mining")

    @format_params(decode_hex, decode_hex)
    async def submitHashrate(self, hashrate: Hash32, id: Hash32) -> NoReturn:
        raise NotImplementedError("Trinity does not support mining")

    @format_params(decode_hex, identity)
    async def getBlockByHash(self,
                             block_hash: Hash32,
                             include_transactions: bool) -> RpcBlockResponse:
        block = await self.chain.coro_get_block_by_hash(block_hash)
        return block_to_dict(block, self.chain, include_transactions)

    @format_params(to_int_if_hex, identity)
    async def getBlockByNumber(self,
                               at_block: Union[str, int],
                               include_transactions: bool) -> RpcBlockResponse:
        block = await get_block_at_number(self.chain, at_block)
        return block_to_dict(block, self.chain, include_transactions)

    @format_params(decode_hex)
    async def getBlockTransactionCountByHash(self, block_hash: Hash32) -> str:
        block = await self.chain.coro_get_block_by_hash(block_hash)
        return hex(len(block.transactions))

    @format_params(to_int_if_hex)
    async def getBlockTransactionCountByNumber(self, at_block: Union[str, int]) -> str:
        block = await get_block_at_number(self.chain, at_block)
        return hex(len(block.transactions))

    @retryable(which_block_arg_name='at_block')
    @format_params(decode_hex, to_int_if_hex)
    async def getCode(self, address: Address, at_block: Union[str, int]) -> str:
        state = await state_at_block(self.chain, at_block)
        code = state.get_code(address)
        return encode_hex(code)

    @retryable(which_block_arg_name='at_block')
    @format_params(decode_hex, to_int_if_hex, to_int_if_hex)
    async def getStorageAt(self, address: Address, position: int, at_block: Union[str, int]) -> str:
        if not is_integer(position) or position < 0:
            raise TypeError("Position of storage must be a whole number, but was: %r" % position)

        state = await state_at_block(self.chain, at_block)
        stored_val = state.get_storage(address, position)

        return encode_hex(pad32(int_to_big_endian(stored_val)))

    @format_params(decode_hex)
    async def getTransactionByHash(self,
                                   transaction_hash: Hash32) -> RpcTransactionResponse:
        transaction = await self.chain.coro_get_canonical_transaction(transaction_hash)
        return transaction_to_dict(transaction)

    @format_params(decode_hex, to_int_if_hex)
    async def getTransactionByBlockHashAndIndex(self,
                                                block_hash: Hash32,
                                                index: int) -> RpcTransactionResponse:
        block = await self.chain.coro_get_block_by_hash(block_hash)
        transaction = block.transactions[index]
        return transaction_to_dict(transaction)

    @format_params(to_int_if_hex, to_int_if_hex)
    async def getTransactionByBlockNumberAndIndex(self,
                                                  at_block: Union[str, int],
                                                  index: int) -> RpcTransactionResponse:
        block = await get_block_at_number(self.chain, at_block)
        transaction = block.transactions[index]
        return transaction_to_dict(transaction)

    @format_params(decode_hex, to_int_if_hex)
    async def getTransactionCount(self, address: Address, at_block: Union[str, int]) -> str:
        state = await state_at_block(self.chain, at_block)
        nonce = state.get_nonce(address)
        return hex(nonce)

    @format_params(decode_hex)
    async def getTransactionReceipt(self,
                                    transaction_hash: Hash32) -> RpcReceiptResponse:

        tx_block_number, tx_index = await self.chain.coro_get_canonical_transaction_index(
            transaction_hash,
        )

        try:
            block_header = await self.chain.coro_get_canonical_block_header_by_number(
                tx_block_number
            )
        except HeaderNotFound as exc:
            raise RpcError(
                f"Block {tx_block_number} is not in the canonical chain"
            ) from exc

        try:
            transaction = await self.chain.coro_get_canonical_transaction_by_index(
                tx_block_number,
                tx_index
            )
        except TransactionNotFound as exc:
            raise RpcError(
                f"Transaction {encode_hex(transaction_hash)} is not in the canonical chain"
            ) from exc

        if transaction.hash != transaction_hash:
            raise RpcError(
                f"Unexpected transaction {encode_hex(transaction.hash)} at index {tx_index}"
            )

        receipt = await self.chain.coro_get_transaction_receipt_by_index(
            tx_block_number,
            tx_index
        )

        if tx_index > 0:
            previous_receipt = await self.chain.coro_get_transaction_receipt_by_index(
                tx_block_number,
                tx_index - 1
            )
            # The receipt only tells us the cumulative gas that was used. To find the gas used by
            # the transaction alone we have to get the previous receipt and calculate the
            # difference.
            tx_gas_used = receipt.gas_used - previous_receipt.gas_used
        else:
            tx_gas_used = receipt.gas_used

        return to_receipt_response(receipt, transaction, tx_index, block_header, tx_gas_used)

    @format_params(decode_hex)
    async def getUncleCountByBlockHash(self, block_hash: Hash32) -> str:
        block = await self.chain.coro_get_block_by_hash(block_hash)
        return hex(len(block.uncles))

    @format_params(to_int_if_hex)
    async def getUncleCountByBlockNumber(self, at_block: Union[str, int]) -> str:
        block = await get_block_at_number(self.chain, at_block)
        return hex(len(block.uncles))

    @format_params(decode_hex, to_int_if_hex)
    async def getUncleByBlockHashAndIndex(self,
                                          block_hash: Hash32,
                                          index: int) -> RpcHeaderResponse:
        block = await self.chain.coro_get_block_by_hash(block_hash)
        uncle = block.uncles[index]
        return header_to_dict(uncle)

    @format_params(to_int_if_hex, to_int_if_hex)
    async def getUncleByBlockNumberAndIndex(self,
                                            at_block: Union[str, int],
                                            index: int) -> RpcHeaderResponse:
        block = await get_block_at_number(self.chain, at_block)
        uncle = block.uncles[index]
        return header_to_dict(uncle)

    async def hashrate(self) -> str:
        raise NotImplementedError("Trinity does not support mining")

    async def mining(self) -> bool:
        return False

    async def protocolVersion(self) -> str:
        return "63"

    async def syncing(self) -> Union[bool, SyncProgressDict]:
        res = await self.event_bus.request(SyncingRequest(), TO_NETWORKING_BROADCAST_CONFIG)
        if res.is_syncing:
            return {
                "startingBlock": res.progress.starting_block,
                "currentBlock": res.progress.current_block,
                "highestBlock": res.progress.highest_block
            }
        return False
