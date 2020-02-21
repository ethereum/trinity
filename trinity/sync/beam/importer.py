from abc import abstractmethod
import asyncio
from concurrent import futures
from operator import attrgetter
from typing import (
    Any,
    Callable,
    Optional,
    Tuple,
    Type,
    TypeVar,
    cast,
)

from async_service import Service

from eth.abc import (
    AtomicDatabaseAPI,
    BlockAPI,
    BlockHeaderAPI,
    ConsensusContextAPI,
    SignedTransactionAPI,
    StateAPI,
    VirtualMachineAPI,
)
from eth.typing import VMConfiguration
from eth.vm.interrupt import (
    MissingAccountTrieNode,
    MissingBytecode,
    MissingStorageTrieNode,
)
from eth_typing import (
    Address,
    BlockNumber,
    Hash32,
)
from eth_utils import (
    ExtendedDebugLogger,
    ValidationError,
    get_extended_debug_logger,
    humanize_seconds,
)
from eth_utils.toolz import (
    groupby,
)

from lahja import EndpointAPI
from lahja.common import BroadcastConfig

from trinity._utils.timer import Timer
from trinity.chains.full import FullChain
from trinity.exceptions import StateUnretrievable
from trinity.sync.beam.constants import (
    MAX_SPECULATIVE_EXECUTIONS_PER_PROCESS,
    NUM_PREVIEW_SHARDS,
)
from trinity.sync.common.events import (
    CollectMissingAccount,
    CollectMissingBytecode,
    CollectMissingStorage,
    DoStatelessBlockImport,
    DoStatelessBlockPreview,
    MissingAccountResult,
    MissingBytecodeResult,
    MissingStorageResult,
    StatelessBlockImportDone,
)

ImportBlockType = Tuple[BlockAPI, Tuple[BlockAPI, ...], Tuple[BlockAPI, ...]]


class BeamStats:
    num_accounts = 0
    num_account_nodes = 0
    num_bytecodes = 0
    num_storages = 0
    num_storage_nodes = 0

    # How much time is spent waiting on retrieving nodes?
    data_pause_time = 0.0

    @property
    def num_nodes(self) -> int:
        return self.num_account_nodes + self.num_bytecodes + self.num_storage_nodes

    def __str__(self) -> str:
        if self.num_nodes:
            avg_rtt = self.data_pause_time / self.num_nodes
        else:
            avg_rtt = 0

        wait_time = humanize_seconds(self.data_pause_time)

        return (
            f"BeamStat: accts={self.num_accounts}, "
            f"a_nodes={self.num_account_nodes}, codes={self.num_bytecodes}, "
            f"strg={self.num_storages}, s_nodes={self.num_storage_nodes}, "
            f"nodes={self.num_nodes}, rtt={avg_rtt:.3f}s, wait={wait_time}"
        )

    def __repr__(self) -> str:
        return (
            f"BeamStats(num_accounts={self.num_accounts}, "
            f"num_account_nodes={self.num_account_nodes}, num_bytecodes={self.num_bytecodes}, "
            f"num_storages={self.num_storages}, num_storage_nodes={self.num_storage_nodes}, "
            f"data_pause_time={self.data_pause_time:.3f}s)"
        )


class PausingVMAPI(VirtualMachineAPI):
    logger: ExtendedDebugLogger

    @abstractmethod
    def get_beam_stats(self) -> BeamStats:
        ...


class BeamChain(FullChain):
    """
    The primary job of this patched BeamChain is to keep track of the
    first VM instance that it creates.

    Stats are attached to the VM that did the importing, and this is
    a way to get access to that particular vm instance that
    imported the block.

    My finest NFT to whoever replaces this with something better...
    """
    _first_vm: PausingVMAPI = None

    def get_vm(self, at_header: BlockHeaderAPI = None) -> PausingVMAPI:
        vm = cast(PausingVMAPI, super().get_vm(at_header))
        if self._first_vm is None:
            self._first_vm = vm
        return vm

    def get_first_vm(self) -> PausingVMAPI:
        if self._first_vm is None:
            return self.get_vm()
        else:
            return self._first_vm

    def clear_first_vm(self) -> None:
        self._first_vm = None


def make_pausing_beam_chain(
        vm_config: VMConfiguration,
        chain_id: int,
        consensus_context_class: Type[ConsensusContextAPI],
        db: AtomicDatabaseAPI,
        event_bus: EndpointAPI,
        loop: asyncio.AbstractEventLoop,
        urgent: bool = True) -> BeamChain:
    """
    Patch the py-evm chain with a VMState that pauses when state data
    is missing, and emits an event which requests the missing data.
    """
    pausing_vm_config = tuple(
        (starting_block, pausing_vm_decorator(vm, event_bus, loop, urgent=urgent))
        for starting_block, vm in vm_config
    )
    PausingBeamChain = BeamChain.configure(
        vm_configuration=pausing_vm_config,
        chain_id=chain_id,
    )
    return PausingBeamChain(db)


TVMFuncReturn = TypeVar('TVMFuncReturn')


def pausing_vm_decorator(
        original_vm_class: Type[VirtualMachineAPI],
        event_bus: EndpointAPI,
        loop: asyncio.AbstractEventLoop,
        urgent: bool = True) -> Type[VirtualMachineAPI]:
    """
    Decorate a py-evm VM so that it will pause when data is missing
    """
    async def request_missing_storage(
            missing_node_hash: Hash32,
            storage_key: Hash32,
            storage_root_hash: Hash32,
            account_address: Address,
            block_number: BlockNumber) -> MissingStorageResult:
        if event_bus.is_any_endpoint_subscribed_to(CollectMissingStorage):
            return await event_bus.request(CollectMissingStorage(
                missing_node_hash,
                storage_key,
                storage_root_hash,
                account_address,
                urgent,
                block_number,
            ))
        else:
            raise StateUnretrievable("No servers for CollectMissingStorage")

    async def request_missing_account(
            missing_node_hash: Hash32,
            address_hash: Hash32,
            state_root_hash: Hash32,
            block_number: BlockNumber) -> MissingAccountResult:
        if event_bus.is_any_endpoint_subscribed_to(CollectMissingAccount):
            return await event_bus.request(CollectMissingAccount(
                missing_node_hash,
                address_hash,
                state_root_hash,
                urgent,
                block_number,
            ))
        else:
            raise StateUnretrievable("No servers for CollectMissingAccount")

    async def request_missing_bytecode(
            bytecode_hash: Hash32,
            block_number: BlockNumber) -> MissingBytecodeResult:
        if event_bus.is_any_endpoint_subscribed_to(CollectMissingBytecode):
            return await event_bus.request(CollectMissingBytecode(
                bytecode_hash,
                urgent,
                block_number,
            ))
        else:
            raise StateUnretrievable("No servers for CollectMissingBytecode")

    class PausingVMState(original_vm_class.get_state_class()):  # type: ignore
        """
        A custom version of VMState that pauses EVM execution when required data is missing.
        """
        stats_counter: BeamStats
        node_retrieval_timeout = 20

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            self.stats_counter = BeamStats()

        def _pause_on_missing_data(
                self,
                vm_method: Callable[[Any], TVMFuncReturn],
                *args: Any,
                **kwargs: Any) -> TVMFuncReturn:
            """
            Catch exceptions about missing state data and pause while waiting for
            the event bus to reply with the needed data. Repeat if there is a request timeout.
            """
            while True:
                try:
                    return self._request_missing_data(vm_method, *args, **kwargs)
                except futures.TimeoutError:
                    self.stats_counter.data_pause_time += self.node_retrieval_timeout

                    if urgent:
                        log_func = self.logger.warning
                    else:
                        log_func = self.logger.debug
                    log_func(
                        "Timed out requsting state data for block #%d, retrying..."
                        " Stats so far: %s",
                        self.block_number,
                        self.stats_counter,
                    )

        def _request_missing_data(
                self,
                vm_method: Callable[[Any], TVMFuncReturn],
                *args: Any,
                **kwargs: Any) -> TVMFuncReturn:
            """
            Catch exceptions about missing state data and pause while waiting for
            the event bus to reply with the needed data.
            """
            while True:
                try:
                    return vm_method(*args, **kwargs)  # type: ignore
                except MissingAccountTrieNode as exc:
                    t = Timer()
                    account_future = asyncio.run_coroutine_threadsafe(
                        request_missing_account(
                            exc.missing_node_hash,
                            exc.address_hash,
                            exc.state_root_hash,
                            self.block_number,
                        ),
                        loop,
                    )
                    account_event = account_future.result(timeout=self.node_retrieval_timeout)

                    # Collect the amount of paused time before checking if we should exit, so
                    #   it shows up in logged statistics.
                    self.stats_counter.data_pause_time += t.elapsed
                    if not account_event.is_retry_acceptable:
                        raise StateUnretrievable("Server asked us to stop trying")
                    self.stats_counter.num_accounts += 1
                    self.stats_counter.num_account_nodes += account_event.num_nodes_collected
                except MissingBytecode as exc:
                    t = Timer()
                    bytecode_future = asyncio.run_coroutine_threadsafe(
                        request_missing_bytecode(
                            exc.missing_code_hash,
                            self.block_number,
                        ),
                        loop,
                    )
                    bytecode_event = bytecode_future.result(timeout=self.node_retrieval_timeout)
                    self.stats_counter.data_pause_time += t.elapsed
                    if not bytecode_event.is_retry_acceptable:
                        raise StateUnretrievable("Server asked us to stop trying")
                    self.stats_counter.num_bytecodes += 1
                except MissingStorageTrieNode as exc:
                    t = Timer()
                    storage_future = asyncio.run_coroutine_threadsafe(
                        request_missing_storage(
                            exc.missing_node_hash,
                            exc.requested_key,
                            exc.storage_root_hash,
                            exc.account_address,
                            self.block_number,
                        ),
                        loop,
                    )
                    storage_event = storage_future.result(timeout=self.node_retrieval_timeout)
                    self.stats_counter.data_pause_time += t.elapsed
                    if not storage_event.is_retry_acceptable:
                        raise StateUnretrievable("Server asked us to stop trying")
                    self.stats_counter.num_storages += 1
                    self.stats_counter.num_storage_nodes += storage_event.num_nodes_collected

        def get_balance(self, account: bytes) -> int:
            return self._pause_on_missing_data(super().get_balance, account)

        def get_code(self, account: bytes) -> bytes:
            return self._pause_on_missing_data(super().get_code, account)

        def get_storage(self, *args: Any, **kwargs: Any) -> int:
            return self._pause_on_missing_data(super().get_storage, *args, **kwargs)

        def delete_storage(self, *args: Any, **kwargs: Any) -> None:
            return self._pause_on_missing_data(super().delete_storage, *args, **kwargs)

        def delete_account(self, *args: Any, **kwargs: Any) -> None:
            return self._pause_on_missing_data(super().delete_account, *args, **kwargs)

        def set_balance(self, *args: Any, **kwargs: Any) -> None:
            return self._pause_on_missing_data(super().set_balance, *args, **kwargs)

        def get_nonce(self, *args: Any, **kwargs: Any) -> int:
            return self._pause_on_missing_data(super().get_nonce, *args, **kwargs)

        def set_nonce(self, *args: Any, **kwargs: Any) -> None:
            return self._pause_on_missing_data(super().set_nonce, *args, **kwargs)

        def increment_nonce(self, *args: Any, **kwargs: Any) -> None:
            return self._pause_on_missing_data(super().increment_nonce, *args, **kwargs)

        def set_code(self, *args: Any, **kwargs: Any) -> None:
            return self._pause_on_missing_data(super().set_code, *args, **kwargs)

        def get_code_hash(self, *args: Any, **kwargs: Any) -> Hash32:
            return self._pause_on_missing_data(super().get_code_hash, *args, **kwargs)

        def delete_code(self, *args: Any, **kwargs: Any) -> None:
            return self._pause_on_missing_data(super().delete_code, *args, **kwargs)

        def has_code_or_nonce(self, *args: Any, **kwargs: Any) -> bool:
            return self._pause_on_missing_data(super().has_code_or_nonce, *args, **kwargs)

        def account_exists(self, *args: Any, **kwargs: Any) -> bool:
            return self._pause_on_missing_data(super().account_exists, *args, **kwargs)

        def touch_account(self, *args: Any, **kwargs: Any) -> None:
            return self._pause_on_missing_data(super().touch_account, *args, **kwargs)

        def account_is_empty(self, *args: Any, **kwargs: Any) -> bool:
            return self._pause_on_missing_data(super().account_is_empty, *args, **kwargs)

        def persist(self) -> Optional[Any]:
            return self._pause_on_missing_data(super().persist)

        def make_state_root(self) -> Optional[Any]:
            return self._pause_on_missing_data(super().make_state_root)

    class PausingVM(original_vm_class):  # type: ignore
        logger = get_extended_debug_logger(f'eth.vm.base.VM.{original_vm_class.__name__}')

        @classmethod
        def get_state_class(cls) -> Type[StateAPI]:
            return PausingVMState

        def get_beam_stats(self) -> BeamStats:
            return self.state.stats_counter

    return PausingVM


def _broadcast_import_complete(
        event_bus: EndpointAPI,
        block: BlockAPI,
        broadcast_config: BroadcastConfig,
        future: 'asyncio.Future[ImportBlockType]') -> None:
    completed = not future.cancelled()
    event_bus.broadcast_nowait(
        StatelessBlockImportDone(
            block,
            completed,
            future.result() if completed else None,
            future.exception() if completed else None,
        ),
        broadcast_config,
    )


def partial_import_block(beam_chain: BeamChain,
                         block: BlockAPI,
                         ) -> Callable[[], Tuple[BlockAPI, Tuple[BlockAPI, ...], Tuple[BlockAPI, ...]]]:  # noqa: E501
    """
    Get an argument-free function that will import the given block.
    """
    def _import_block() -> Tuple[BlockAPI, Tuple[BlockAPI, ...], Tuple[BlockAPI, ...]]:
        t = Timer()
        beam_chain.clear_first_vm()
        try:
            reorg_info = beam_chain.import_block(block, perform_validation=True)
        except StateUnretrievable as exc:
            import_time = t.elapsed

            vm = beam_chain.get_first_vm()
            beam_stats = vm.get_beam_stats()
            vm.logger.debug(
                "Beam pivot over %s (%d txns) because %r after %.1fs, %%exec %.0f, stats: %s",
                block.header,
                len(block.transactions),
                exc,
                import_time,
                100 * (import_time - beam_stats.data_pause_time) / import_time,
                beam_stats,
            )
            raise
        else:
            import_time = t.elapsed

            vm = beam_chain.get_first_vm()
            beam_stats = vm.get_beam_stats()
            beam_chain.logger.debug(
                "BeamImport %s (%d txns) total time: %.1f s, %%exec %.0f, stats: %s",
                block.header,
                len(block.transactions),
                import_time,
                100 * (import_time - beam_stats.data_pause_time) / import_time,
                beam_stats,
            )
            return reorg_info

    return _import_block


class BlockImportServer(Service):
    logger = get_extended_debug_logger('trinity.sync.beam.BlockImportServer')

    def __init__(
            self,
            event_bus: EndpointAPI,
            beam_chain: BeamChain) -> None:
        self._event_bus = event_bus
        self._beam_chain = beam_chain

    async def run(self) -> None:
        self.manager.run_daemon_task(self.serve, self._event_bus, self._beam_chain)
        await self.manager.wait_finished()

    async def serve(
            self,
            event_bus: EndpointAPI,
            beam_chain: BeamChain) -> None:
        """
        Listen to DoStatelessBlockImport events, and import block when received.
        Reply with StatelessBlockImportDone when import is complete.
        """

        loop = asyncio.get_event_loop()
        async for event in event_bus.stream(DoStatelessBlockImport):
            # launch in new thread, so we don't block the event loop!
            import_completion = loop.run_in_executor(
                # Maybe build the pausing chain inside the new process?
                None,
                partial_import_block(beam_chain, event.block),
            )

            # Wrapped in `asyncio.shield` because we want to hang the service from
            #   shutting down until block import is complete.
            # In the tests, for example, we await cancel() this service, so that we know
            #   that the in-progress block is complete. Then below, we do not send back
            #   the import completion (so the import server won't get triggered again).
            try:
                await asyncio.shield(import_completion)
            except StateUnretrievable as exc:
                self.logger.debug(
                    "Not broadcasting about %s Beam import. Listening for next request, because %r",
                    event.block,
                    exc
                )
            else:
                if self.manager.is_running:
                    _broadcast_import_complete(
                        event_bus,
                        event.block,
                        event.broadcast_config(),
                        import_completion,  # type: ignore
                    )

            if not self.manager.is_running:
                break


def partial_trigger_missing_state_downloads(
        beam_chain: BeamChain,
        header: BlockHeaderAPI,
        transactions: Tuple[SignedTransactionAPI, ...]) -> Callable[[], None]:
    """
    Get an argument-free function that will trigger missing state downloads,
    by executing all the transactions, in the context of the given header.
    """
    def _trigger_missing_state_downloads() -> None:
        vm = beam_chain.get_vm(header)
        unused_header = header.copy(gas_used=0)

        # this won't actually save the results, but all we need to do is generate the trie requests
        t = Timer()
        try:
            vm.apply_all_transactions(transactions, unused_header)
            # Making the state root can also trigger missing trie nodes, so keep it in the try block
            vm.state.make_state_root()
            # We don't need to persist trie nodes here, they are persisted by the BeamDownloader.
        except StateUnretrievable as exc:
            preview_time = t.elapsed
            beam_stats = vm.get_beam_stats()
            vm.logger.debug(
                "Beam pivot over all %d txns for %s b/c %r after %.1fs, %%exec %.0f, stats: %s",
                len(transactions),
                header,
                exc,
                preview_time,
                100 * (preview_time - beam_stats.data_pause_time) / preview_time,
                beam_stats,
            )
        except ValidationError as exc:
            preview_time = t.elapsed
            vm.logger.debug(
                "Preview of all %d transactions failed for %s after %.1fs: %s",
                len(transactions),
                header,
                preview_time,
                exc,
            )
        except OSError:
            vm.logger.info("Got OSError while Beam Sync previewed execution of %s", header)
        except BrokenPipeError:
            vm.logger.info("Got BrokenPipeError while Beam Sync previewed execution of %s", header)
            vm.logger.debug("BrokenPipeError trace for %s", header, exc_info=True)
        else:
            preview_time = t.elapsed

            beam_stats = vm.get_beam_stats()
            vm.logger.debug(
                "Previewed %d transactions for %s in %.1f s, %%exec %.0f, stats: %s",
                len(transactions),
                header,
                preview_time,
                100 * (preview_time - beam_stats.data_pause_time) / preview_time,
                beam_stats,
            )

    return _trigger_missing_state_downloads


def partial_speculative_execute(
        beam_chain: BeamChain,
        header: BlockHeaderAPI,
        transactions: Tuple[SignedTransactionAPI, ...]) -> Callable[[], None]:
    """
    Get an argument-free function that will trigger missing state downloads,
    by executing all the transactions, in the context of the given header.
    """

    def _trigger_missing_state_downloads() -> None:
        vm = beam_chain.get_vm(header)
        unused_header = header.copy(gas_used=0)

        # this won't actually save the results, but all we need to do is generate the trie requests
        t = Timer()
        try:
            _, receipts, _ = vm.apply_all_transactions(transactions, unused_header)
        except StateUnretrievable as exc:
            preview_time = t.elapsed
            beam_stats = vm.get_beam_stats()
            vm.logger.debug(
                "Beam pivot over %d txn preview for %s b/c %r after %.1fs, %%exec %.0f, stats: %s",
                len(transactions),
                header,
                exc,
                preview_time,
                100 * (preview_time - beam_stats.data_pause_time) / preview_time,
                beam_stats,
            )
        except ValidationError as exc:
            preview_time = t.elapsed
            vm.logger.debug(
                "Speculative transactions %s failed for %s after %.1fs: %s",
                transactions,
                header,
                preview_time,
                exc,
            )
        except OSError:
            vm.logger.debug("Got OSError while Beam Sync speculatively executed %s", header)
        except BrokenPipeError:
            vm.logger.debug("Got BrokenPipeError while Beam Sync speculatively executed %s", header)
        else:
            preview_time = t.elapsed

            beam_stats = vm.get_beam_stats()
            vm.logger.debug2(
                "Speculative transaction (%d/%d gas) for %s in %.1f s, %%exec %.0f, stats: %s",
                sum(r.gas_used for r in receipts),
                sum(txn.gas for txn in transactions),
                header,
                preview_time,
                100 * (preview_time - beam_stats.data_pause_time) / preview_time,
                beam_stats,
            )

    return _trigger_missing_state_downloads


class BlockPreviewServer(Service):
    logger = get_extended_debug_logger('trinity.sync.beam.BlockPreviewServer')

    def __init__(
            self,
            event_bus: EndpointAPI,
            beam_chain: BeamChain,
            shard_num: int) -> None:
        self._event_bus = event_bus
        self._beam_chain = beam_chain

        if shard_num < 0 or shard_num >= NUM_PREVIEW_SHARDS:
            raise ValidationError(
                f"Can only run up to {NUM_PREVIEW_SHARDS}, tried to run {shard_num}"
            )
        else:
            self._shard_num = shard_num

    async def run(self) -> None:
        self.manager.run_daemon_task(self.serve, self._event_bus, self._beam_chain)
        await self.manager.wait_finished()

    async def serve(
            self,
            event_bus: EndpointAPI,
            beam_chain: BeamChain) -> None:
        """
        Listen to DoStatelessBlockPreview events, and execute the transactions to prefill
        all the needed state data.
        """
        with futures.ThreadPoolExecutor(
            max_workers=MAX_SPECULATIVE_EXECUTIONS_PER_PROCESS,
            thread_name_prefix="trinity-spec-exec-",
        ) as speculative_thread_executor:

            async for event in event_bus.stream(DoStatelessBlockPreview):
                if event.header.block_number % NUM_PREVIEW_SHARDS != self._shard_num:
                    continue

                self.logger.debug(
                    "DoStatelessBlockPreview-%d is previewing new block: %s",
                    self._shard_num,
                    event.header,
                )
                # Parallel Execution:
                # Run a complete block end-to-end
                asyncio.get_event_loop().run_in_executor(
                    # Maybe build the pausing chain inside the new process,
                    # so we can use process pool?
                    None,
                    partial_trigger_missing_state_downloads(
                        beam_chain,
                        event.header,
                        event.transactions,
                    )
                )

                # Speculative Execution:
                # Split transactions into groups by sender, and run them independently.
                # This effectively assumes that the transactions by each sender are not
                #   affected by any other transactions in the block. This is often true,
                #   so it helps speed up the search for data.
                # Being able to retrieve this predicted data in parallel, asking for more
                # trie nodes in each GetNodeData request, can help make the difference
                # between keeping up and falling behind, on the network.
                transaction_groups = groupby(attrgetter('sender'), event.transactions)
                for sender_transactions in transaction_groups.values():
                    asyncio.get_event_loop().run_in_executor(
                        speculative_thread_executor,
                        partial_speculative_execute(
                            beam_chain,
                            event.header,
                            sender_transactions,
                        )
                    )
                # we don't need to broadcast that the preview is complete, so immediately
                # look for next preview request. That way, we can run them in parallel.
