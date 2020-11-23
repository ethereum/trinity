from dataclasses import (
    dataclass,
)
from typing import (
    Optional,
    Tuple,
    Type,
)

from eth.abc import (
    BlockAPI,
    BlockImportResult,
    BlockHeaderAPI,
    SignedTransactionAPI,
)

from eth_typing import (
    Address,
    BlockNumber,
    Hash32,
)
from lahja import (
    BaseEvent,
    BaseRequestResponseEvent,
)

from p2p.abc import SessionAPI
from trinity.sync.common.types import (
    SyncProgress
)


@dataclass
class SendLocalTransaction(BaseEvent):
    transaction: SignedTransactionAPI


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
    block_number: BlockNumber

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
    block_number: BlockNumber

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
    block_number: BlockNumber

    @staticmethod
    def expected_response_type() -> Type[MissingStorageResult]:
        return MissingStorageResult


@dataclass
class MissingTrieNodesResult(BaseEvent):
    num_nodes_collected: int = 0


@dataclass
class CollectMissingTrieNodes(BaseRequestResponseEvent[MissingTrieNodesResult]):
    """
    A request for the syncer to download the trie nodes with the given hashes.

    Generally the type-specific events like CollectMissingStorage should be used but in some cases
    (i.e. when downloading block witnesses) we don't know their type so need to use this.
    """
    node_hashes: Tuple[Hash32, ...]
    urgent: bool
    block_number: BlockNumber

    @staticmethod
    def expected_response_type() -> Type[MissingTrieNodesResult]:
        return MissingTrieNodesResult


@dataclass
class BlockWitnessResult(BaseEvent):
    witness_hashes: Tuple[Hash32, ...] = tuple()


@dataclass
class FetchBlockWitness(BaseRequestResponseEvent[BlockWitnessResult]):
    preferred_peer: SessionAPI
    hash: Hash32
    number: BlockNumber

    @staticmethod
    def expected_response_type() -> Type[BlockWitnessResult]:
        return BlockWitnessResult


@dataclass
class StatelessBlockImportDone(BaseEvent):
    """
    Response to :cls:`DoStatelessBlockImport`, emitted only after the block has
    been fully imported. This event is emitted whether the import was successful
    or a failure.
    """

    block: BlockAPI
    completed: bool
    result: BlockImportResult
    exception: BaseException


@dataclass
class NewBlockImported(BaseEvent):
    """
    Event that is only emitted after a new block has been successfully imported.
    """

    block: BlockAPI


@dataclass
class DoStatelessBlockImport(BaseRequestResponseEvent[StatelessBlockImportDone]):
    """
    The syncer emits this event when it would like the Beam Sync process to
    start attempting a block import.
    """
    block: BlockAPI

    @staticmethod
    def expected_response_type() -> Type[StatelessBlockImportDone]:
        return StatelessBlockImportDone


@dataclass
class DoStatelessBlockPreview(BaseEvent):
    """
    Event to identify and download the data needed to execute the given transactions
    """
    header: BlockHeaderAPI
    transactions: Tuple[SignedTransactionAPI, ...]
