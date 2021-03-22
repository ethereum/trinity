import cachetools.func

from typing import Type

from eth_typing import (
    BlockNumber,
)

from eth_utils import (
    encode_hex,
    ValidationError,
)

from eth.abc import (
    ChainAPI,
    SignedTransactionAPI,
    TransactionBuilderAPI,
)
from eth.chains.mainnet import MUIR_GLACIER_MAINNET_BLOCK
from eth.chains.ropsten import MUIR_GLACIER_ROPSTEN_BLOCK
from eth.chains.goerli import ISTANBUL_GOERLI_BLOCK

from trinity.constants import (
    GOERLI_NETWORK_ID,
    MAINNET_NETWORK_ID,
    ROPSTEN_NETWORK_ID
)
from trinity.rlp.sedes import UninterpretedTransaction


class DefaultTransactionValidator():
    """
    The :class:`~trinity.tx_pool.validators.DefaultTransactionValidator` class is responsible to
    decide wether transactions should be relayed to peers or not. This implementation validates
    transactions against a transaction class inferred from a ``initial_tx_validation_block_number``
    but will switch to a different one as soon as the tip of the chain uses a more up to date
    transaction class than the one that corresponds to the ``initial_tx_validation_block_number``.
    """

    def __init__(self,
                 chain: ChainAPI,
                 initial_tx_validation_block_number: BlockNumber = None) -> None:
        if not chain.vm_configuration:
            raise TypeError(
                "The `DefaultTransactionValidator` cannot function with an "
                "empty vm_configuration"
            )

        self.chain = chain
        self.vm_configuration = self.chain.vm_configuration

        self._ordered_tx_builders = tuple(
            vm_class.get_transaction_builder()
            for _, vm_class in self.vm_configuration
        )

        if initial_tx_validation_block_number is not None:
            self._initial_tx_builder = self._get_tx_builder_for_block_number(
                initial_tx_validation_block_number
            )
        else:
            self._initial_tx_builder = self._ordered_tx_builders[-1]

        self._initial_tx_builder_index = self._ordered_tx_builders.index(self._initial_tx_builder)

    @classmethod
    def from_network_id(cls, chain: ChainAPI, network_id: int) -> 'DefaultTransactionValidator':
        if network_id == MAINNET_NETWORK_ID:
            return cls(chain, MUIR_GLACIER_MAINNET_BLOCK)
        elif network_id == ROPSTEN_NETWORK_ID:
            return cls(chain, MUIR_GLACIER_ROPSTEN_BLOCK)
        elif network_id == GOERLI_NETWORK_ID:
            return cls(chain, ISTANBUL_GOERLI_BLOCK)
        else:
            raise NotImplementedError(f"Unsupported network id {network_id}")

    def __call__(self, transaction: UninterpretedTransaction) -> bool:

        try:
            self.validate(transaction)
        except ValidationError:
            return False
        else:
            return True

    def validate(self, transaction: UninterpretedTransaction) -> SignedTransactionAPI:
        transaction_builder = self.get_appropriate_tx_builder()
        tx = transaction_builder.deserialize(transaction)
        tx.validate()

        if tx.chain_id != self.chain.chain_id:
            raise ValidationError(
                f"Transaction {encode_hex(tx.hash)} is for chain with id {tx.chain_id} "
                f"but current chain has id {self.chain.chain_id}"
            )
        else:
            return tx

    @cachetools.func.ttl_cache(maxsize=1024, ttl=300)
    def get_appropriate_tx_builder(self) -> Type[TransactionBuilderAPI]:
        head = self.chain.get_canonical_head()
        current_tx_builder = self.chain.get_vm_class(head).get_transaction_builder()

        # If the current head of the chain is still on a fork that is before the currently
        # active fork (syncing), ensure that we use the specified initial tx class
        if self.is_outdated_tx_builder(current_tx_builder):
            return self._initial_tx_builder

        return current_tx_builder

    def is_outdated_tx_builder(self, tx_builder: Type[TransactionBuilderAPI]) -> bool:
        return self._ordered_tx_builders.index(tx_builder) < self._initial_tx_builder_index

    def _get_tx_builder_for_block_number(
            self,
            block_number: BlockNumber) -> Type[TransactionBuilderAPI]:

        vm_class = self.chain.get_vm_class_for_block_number(block_number)
        return vm_class.get_transaction_builder()
