import functools
from typing import (
    Any,
    Callable,
    cast,
    Dict,
    Sequence,
    Union,
)
from eth_utils.toolz import (
    compose,
    merge,
)

from eth_utils import (
    apply_formatter_if,
    apply_formatters_to_dict,
    decode_hex,
    encode_hex,
    int_to_big_endian,
    is_address,
    to_checksum_address,
)

import rlp

from eth.abc import (
    BlockAPI,
    BlockHeaderAPI,
    ReceiptAPI,
    SignedTransactionAPI,
)
from eth.constants import (
    CREATE_CONTRACT_ADDRESS,
)

from trinity.chains.base import AsyncChainAPI
from trinity.rpc.typing import (
    RpcBlockResponse,
    RpcBlockTransactionResponse,
    RpcHeaderResponse,
    RpcReceiptResponse,
    RpcTransactionResponse,
)
from trinity._utils.address import generate_contract_address


def format_bloom(bloom: int) -> str:
    formatted_bloom = encode_hex(int_to_big_endian(bloom))[2:]
    formatted_bloom = '0x' + formatted_bloom.rjust(512, '0')
    return formatted_bloom


def to_receipt_response(receipt: ReceiptAPI,
                        transaction: SignedTransactionAPI,
                        index: int,
                        header: BlockHeaderAPI,
                        tx_gas_used: int) -> RpcReceiptResponse:

    if transaction.to == CREATE_CONTRACT_ADDRESS:
        contract_address = encode_hex(
            generate_contract_address(transaction.sender, transaction.nonce)
        )
    else:
        contract_address = None

    block_hash = encode_hex(header.hash)
    block_number = hex(header.block_number)
    receipt_and_transaction_index = hex(index)
    transaction_hash = encode_hex(transaction.hash)

    return {
        "blockHash": block_hash,
        "blockNumber": block_number,
        "contractAddress": contract_address,
        "cumulativeGasUsed": hex(receipt.gas_used),
        "from": encode_hex(transaction.sender),
        'gasUsed': hex(tx_gas_used),
        "logs": [
            {
                "address": encode_hex(log.address),
                "data": encode_hex(log.data),
                "blockHash": block_hash,
                "blockNumber": block_number,
                "logIndex": receipt_and_transaction_index,
                # We only serve receipts from transactions that ended up in the canonical chain
                # which means this can never be `True`
                "removed": False,
                "topics": [
                    encode_hex(int_to_big_endian(topic)) for topic in log.topics
                ],
                "transactionHash": transaction_hash,
                "transactionIndex": receipt_and_transaction_index,
            }
            for log in receipt.logs
        ],
        "logsBloom": format_bloom(receipt.bloom),
        "root": encode_hex(receipt.state_root),
        "to": encode_hex(transaction.to),
        "transactionHash": transaction_hash,
        "transactionIndex": receipt_and_transaction_index,
    }


def transaction_to_dict(transaction: SignedTransactionAPI) -> RpcTransactionResponse:
    return {
        'hash': encode_hex(transaction.hash),
        'nonce': hex(transaction.nonce),
        'gas': hex(transaction.gas),
        'gasPrice': hex(transaction.gas_price),
        'from': to_checksum_address(transaction.sender),
        'to': apply_formatter_if(
            is_address,
            to_checksum_address,
            encode_hex(transaction.to)
        ),
        'value': hex(transaction.value),
        'input': encode_hex(transaction.data),
        'r': hex(transaction.r),
        's': hex(transaction.s),
        'v': hex(transaction.v),
    }


def block_transaction_to_dict(transaction: SignedTransactionAPI,
                              header: BlockHeaderAPI) -> RpcBlockTransactionResponse:
    data = cast(RpcBlockTransactionResponse, transaction_to_dict(transaction))
    data['blockHash'] = encode_hex(header.hash)
    data['blockNumber'] = hex(header.block_number)

    return data


hexstr_to_int = functools.partial(int, base=16)


TRANSACTION_NORMALIZER = {
    'data': decode_hex,
    'from': decode_hex,
    'gas': hexstr_to_int,
    'gasPrice': hexstr_to_int,
    'nonce': hexstr_to_int,
    'to': decode_hex,
    'value': hexstr_to_int,
}

SAFE_TRANSACTION_DEFAULTS = {
    'data': b'',
    'to': CREATE_CONTRACT_ADDRESS,
    'value': 0,
}


def normalize_transaction_dict(transaction_dict: Dict[str, str]) -> Dict[str, Any]:
    normalized_dict = apply_formatters_to_dict(TRANSACTION_NORMALIZER, transaction_dict)
    return merge(SAFE_TRANSACTION_DEFAULTS, normalized_dict)


def header_to_dict(header: BlockHeaderAPI) -> RpcHeaderResponse:
    return {
        "difficulty": hex(header.difficulty),
        "extraData": encode_hex(header.extra_data),
        "gasLimit": hex(header.gas_limit),
        "gasUsed": hex(header.gas_used),
        "hash": encode_hex(header.hash),
        "logsBloom": format_bloom(header.bloom),
        "mixHash": encode_hex(header.mix_hash),
        "nonce": encode_hex(header.nonce),
        "number": hex(header.block_number),
        "parentHash": encode_hex(header.parent_hash),
        "receiptsRoot": encode_hex(header.receipt_root),
        "sha3Uncles": encode_hex(header.uncles_hash),
        "stateRoot": encode_hex(header.state_root),
        "timestamp": hex(header.timestamp),
        "transactionsRoot": encode_hex(header.transaction_root),
        "miner": encode_hex(header.coinbase),
    }


def block_to_dict(block: BlockAPI,
                  chain: AsyncChainAPI,
                  include_transactions: bool) -> RpcBlockResponse:

    # There doesn't seem to be a type safe way to initialize the RpcBlockResponse from
    # a RpcHeaderResponse + the extra fields hence the cast here.
    response = cast(RpcBlockResponse, header_to_dict(block.header))

    if include_transactions:
        txs: Union[Sequence[str], Sequence[RpcBlockTransactionResponse]] = [
            block_transaction_to_dict(tx, block.header) for tx in block.transactions
        ]
    else:
        txs = [encode_hex(tx.hash) for tx in block.transactions]

    response['totalDifficulty'] = hex(chain.get_score(block.hash))
    response['uncles'] = [encode_hex(uncle.hash) for uncle in block.uncles]
    response['size'] = hex(len(rlp.encode(block)))
    response['transactions'] = txs

    return response


def format_params(*formatters: Any) -> Callable[..., Any]:
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @functools.wraps(func)
        async def formatted_func(self: Any, *args: Any) -> Callable[..., Any]:
            if len(formatters) != len(args):
                raise TypeError("could not apply %d formatters to %r" % (len(formatters), args))
            formatted = (formatter(arg) for formatter, arg in zip(formatters, args))
            return await func(self, *formatted)
        return formatted_func
    return decorator


def to_int_if_hex(value: Any) -> Any:
    if isinstance(value, str) and value.startswith('0x'):
        return int(value, 16)
    else:
        return value


def empty_to_0x(val: str) -> str:
    if val:
        return val
    else:
        return '0x'


remove_leading_zeros = compose(hex, functools.partial(int, base=16))
