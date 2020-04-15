from abc import ABC, abstractmethod
from typing import (
    Tuple,
)

from eth_typing import (
    Hash32,
)

from eth.abc import (
    BlockAPI,
    BlockHeaderAPI,
    SignedTransactionAPI,
)

from eth2.beacon.types.blocks import BaseBeaconBlock

from trinity.chains.base import AsyncChainAPI

from eth2.beacon.chains.base import (
    BaseBeaconChain
)


class BaseBlockImporter(ABC):
    @abstractmethod
    async def import_block(
            self,
            block: BlockAPI) -> Tuple[BlockAPI, Tuple[BlockAPI, ...], Tuple[BlockAPI, ...]]:
        ...

    async def preview_transactions(
            self,
            header: BlockHeaderAPI,
            transactions: Tuple[SignedTransactionAPI, ...],
            parent_state_root: Hash32,
            lagging: bool = True) -> None:
        """
        Give the importer a chance to preview upcoming blocks. This can improve performance

        :param header: The header of the upcoming block
        :param transactions: The transactions in the upcoming block
        :param parent_state_root: The state root hash at the beginning of the upcoming block
            (the end of the previous block)
        :param lagging: Is the upcoming block *very* far ahead of the current block?

        The lagging parameter is used to take actions that may be resource-intensive and slow,
        but will accelerate the block once we catch up to it. A slow preparation is a waste of
        resources unless the upcoming block is far enough in the future.
        """
        # default action: none
        pass


class SimpleBlockImporter(BaseBlockImporter):
    def __init__(self, chain: AsyncChainAPI) -> None:
        self._chain = chain

    async def import_block(
            self,
            block: BlockAPI) -> Tuple[BlockAPI, Tuple[BlockAPI, ...], Tuple[BlockAPI, ...]]:
        return await self._chain.coro_import_block(block, perform_validation=True)


class BaseSyncBlockImporter(ABC):
    @abstractmethod
    def import_block(
            self,
            block: BlockAPI) -> Tuple[BlockAPI, Tuple[BlockAPI, ...], Tuple[BlockAPI, ...]]:
        ...


class SyncBlockImporter(BaseSyncBlockImporter):
    def __init__(self, chain: BaseBeaconChain) -> None:
        self._chain = chain

    def import_block(
            self,
            block: BaseBeaconBlock
    ) -> Tuple[BaseBeaconBlock, Tuple[BaseBeaconBlock, ...], Tuple[BaseBeaconBlock, ...]]:
        return self._chain.import_block(block, perform_validation=True)
