from typing import (
    Any,
    cast,
    Dict,
)

from eth.abc import (
    SignedTransactionAPI,
    StateAPI,
)
from eth.constants import (
    ZERO_ADDRESS,
)
from eth.rlp.blocks import (
    BaseBlock,
)
from eth.vm.spoof import (
    SpoofTransaction,
)

from trinity.rpc.format import (
    normalize_transaction_dict,
)


from typing import (
    Union,
)

from eth_typing import (
    BlockNumber,
)
from eth_utils import (
    is_integer,
)

from eth.rlp.headers import (
    BlockHeader,
)

from trinity.chains.base import AsyncChainAPI


async def get_header(chain: AsyncChainAPI, at_block: Union[str, int]) -> BlockHeader:
    if at_block == 'pending':
        raise NotImplementedError("RPC interface does not support the 'pending' block at this time")
    elif at_block == 'latest':
        at_header = chain.get_canonical_head()
    elif at_block == 'earliest':
        # TODO find if genesis block can be non-zero. Why does 'earliest' option even exist?
        block = await chain.coro_get_canonical_block_by_number(BlockNumber(0))
        at_header = block.header
    # mypy doesn't have user defined type guards yet
    # https://github.com/python/mypy/issues/5206
    elif is_integer(at_block) and at_block >= 0:  # type: ignore
        block = await chain.coro_get_canonical_block_by_number(BlockNumber(int(at_block)))
        at_header = block.header
    else:
        raise TypeError("Unrecognized block reference: %r" % at_block)

    return at_header


async def state_at_block(
        chain: AsyncChainAPI,
        at_block: Union[str, int],
        read_only: bool=True) -> StateAPI:
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
        transaction_dict: Dict[str, Any],
        normalize_transaction=normalize_transaction_dict
) -> SignedTransactionAPI:
    """
    Convert dicts used in calls & gas estimates into a spoof transaction
    """
    txn_dict = normalize_transaction(transaction_dict)
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
