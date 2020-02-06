from dataclasses import (
    dataclass,
)
from typing import (
    Optional,
    Tuple,
    Type,
)

from eth.rlp.blocks import BaseBlock
from eth.rlp.headers import BlockHeader
from eth.rlp.transactions import BaseTransaction
from eth_typing import (
    Address,
    Hash32,
)
from lahja import (
    BaseEvent,
    BaseRequestResponseEvent,
)

from trinity.sync.common.types import (
    SyncProgress
)


@dataclass
class SyncingResponse(BaseEvent):
    is_syncing: bool
    progress: Optional[SyncProgress]


class SyncingRequest(BaseRequestResponseEvent[SyncingResponse]):
    @staticmethod
    def expected_response_type() -> Type[SyncingResponse]:
        return SyncingResponse


@dataclass
class MissingAccountResult(BaseEvent):
    """
    Response to :cls:`CollectMissingAccount`, emitted only after the account has
    been downloaded from a peer, and can be retrieved in the database.
    """
    num_nodes_collected: int = 0
    is_retry_acceptable: bool = True


@dataclass
class CollectMissingAccount(BaseRequestResponseEvent[MissingAccountResult]):
    """
    Beam Sync has been paused because the given address and/or missing_node_hash
    is missing from the state DB, at the given state root hash.
    """
    missing_node_hash: Hash32
    address_hash: Hash32
    state_root_hash: Hash32
    urgent: bool
    block_number: int

    @staticmethod
    def expected_response_type() -> Type[MissingAccountResult]:
        return MissingAccountResult


@dataclass
class MissingBytecodeResult(BaseEvent):
    """
    Response to :cls:`CollectMissingBytecode`, emitted only after the bytecode has
    been downloaded from a peer, and can be retrieved in the database.
    """
    is_retry_acceptable: bool = True


@dataclass
class CollectMissingBytecode(BaseRequestResponseEvent[MissingBytecodeResult]):
    """
    Beam Sync has been paused because the given bytecode
    is missing from the state DB, at the given state root hash.
    """
    bytecode_hash: Hash32
    urgent: bool
    block_number: int

    @staticmethod
    def expected_response_type() -> Type[MissingBytecodeResult]:
        return MissingBytecodeResult


@dataclass
class MissingStorageResult(BaseEvent):
    """
    Response to :cls:`CollectMissingStorage`, emitted only after the storage value has
    been downloaded from a peer, and can be retrieved in the database.
    """
    num_nodes_collected: int = 0
    is_retry_acceptable: bool = True


@dataclass
class CollectMissingStorage(BaseRequestResponseEvent[MissingStorageResult]):
    """
    Beam Sync has been paused because the given storage key and/or missing_node_hash
    is missing from the state DB, at the given state root hash.
    """

    missing_node_hash: Hash32
    storage_key: Hash32
    storage_root_hash: Hash32
    account_address: Address
    urgent: bool
    block_number: int

    @staticmethod
    def expected_response_type() -> Type[MissingStorageResult]:
        return MissingStorageResult


@dataclass
class StatelessBlockImportDone(BaseEvent):
    """
    Response to :cls:`DoStatelessBlockImport`, emitted only after the block has
    been fully imported. This event is emitted whether the import was successful
    or a failure.
    """

    block: BaseBlock
    completed: bool
    result: Tuple[BaseBlock, Tuple[BaseBlock, ...], Tuple[BaseBlock, ...]]
    # flake8 gets confused by the Tuple syntax above
    exception: BaseException  # noqa: E701


@dataclass
class DoStatelessBlockImport(BaseRequestResponseEvent[StatelessBlockImportDone]):
    """
    The syncer emits this event when it would like the Beam Sync process to
    start attempting a block import.
    """
    block: BaseBlock

    @staticmethod
    def expected_response_type() -> Type[StatelessBlockImportDone]:
        return StatelessBlockImportDone


@dataclass
class DoStatelessBlockPreview(BaseEvent):
    """
    Event to identify and download the data needed to execute the given transactions
    """
    header: BlockHeader
    transactions: Tuple[BaseTransaction, ...]
