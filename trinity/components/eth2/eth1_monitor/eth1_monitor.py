import bisect
from collections import OrderedDict
import logging
from typing import Any, AsyncGenerator, Callable, Sequence, Tuple, Type, TypeVar, Union

from async_service import Service
from eth_typing import BlockNumber, Hash32

from lahja import EndpointAPI
import trio
from web3 import Web3

from eth.abc import AtomicDatabaseAPI
from eth.exceptions import BlockNotFound

from eth_utils import humanize_hash

from eth2.beacon.typing import Timestamp
from eth2.beacon.types.deposits import Deposit
from eth2.beacon.types.deposit_data import DepositData
from eth2.beacon.types.eth1_data import Eth1Data
from eth2.beacon.tools.builder.validator import (
    make_deposit_proof,
    make_deposit_tree_and_root,
)

from .db import BaseDepositDataDB, ListCachedDepositDataDB
from .eth1_data_provider import BaseEth1DataProvider, DepositLog, Eth1Block
from .events import (
    GetDistanceRequest,
    GetDistanceResponse,
    GetDepositResponse,
    GetDepositRequest,
    GetEth1DataRequest,
    GetEth1DataResponse,
)
from .exceptions import (
    DepositDataCorrupted,
    Eth1BlockNotFound,
    Eth1MonitorValidationError,
)


TRequest = TypeVar("TRequest", bound=Union[GetDepositRequest, GetEth1DataRequest])


def _w3_get_block(w3: Web3, *args: Any, **kwargs: Any) -> Eth1Block:
    block_dict = w3.eth.getBlock(*args, **kwargs)
    return Eth1Block(
        block_hash=Hash32(block_dict["hash"]),
        number=BlockNumber(block_dict["number"]),
        timestamp=Timestamp(block_dict["timestamp"]),
    )


class Eth1Monitor(Service):
    logger = logging.getLogger("trinity.components.eth2.eth1_monitor.Eth1Monitor")

    _eth1_data_provider: BaseEth1DataProvider

    # Number of blocks we wait to consider a block is "confirmed". This is used to avoid
    # mainchain forks.
    # We always get a `block` and parse the logs from it, where
    # `block.number <= latest_block.number - _num_blocks_confirmed`.
    _num_blocks_confirmed: int
    # Time period that we poll latest blocks from web3.
    _polling_period: float

    _event_bus: EndpointAPI

    # DB storing `DepositData` we have received so far.
    _db: BaseDepositDataDB
    # Mapping from `block.timestamp` to `block.number`.
    _block_timestamp_to_number: "OrderedDict[Timestamp, BlockNumber]"
    # largest block timestamp among the blocks we have processed
    _largest_block_timestamp: Timestamp

    def __init__(
        self,
        *,
        eth1_data_provider: BaseEth1DataProvider,
        num_blocks_confirmed: int,
        polling_period: float,
        start_block_number: BlockNumber,
        event_bus: EndpointAPI,
        base_db: AtomicDatabaseAPI,
    ) -> None:
        self._eth1_data_provider = eth1_data_provider
        self._num_blocks_confirmed = num_blocks_confirmed
        self._polling_period = polling_period
        self._event_bus = event_bus
        self._db: BaseDepositDataDB = ListCachedDepositDataDB(
            base_db, BlockNumber(start_block_number - 1)
        )

        self._block_timestamp_to_number = OrderedDict()
        self._largest_block_timestamp = None

    @property
    def total_deposit_count(self) -> int:
        return self._db.deposit_count

    @property
    def highest_processed_block_number(self) -> BlockNumber:
        return self._db.highest_processed_block_number

    async def run(self) -> None:
        self.logger.info("Eth1 Monitor up")
        self.manager.run_daemon_task(self._handle_new_logs)
        self.manager.run_daemon_task(
            self._run_handle_request, *(GetDistanceRequest, self._handle_get_distance)
        )
        self.manager.run_daemon_task(
            self._run_handle_request, *(GetDepositRequest, self._handle_get_deposit)
        )
        self.manager.run_daemon_task(
            self._run_handle_request, *(GetEth1DataRequest, self._handle_get_eth1_data)
        )
        await self.manager.wait_finished()

    def _handle_get_distance(self, req: GetDistanceRequest) -> GetDistanceResponse:
        """
        Handle requests for `get_distance` from the event bus.
        """
        try:
            block = self._eth1_data_provider.get_block(req.block_hash)
        except BlockNotFound:
            raise Eth1MonitorValidationError(
                f"Block does not exist for block_hash={humanize_hash(req.block_hash)}"
            )
        eth1_voting_period_start_block_number = self._get_closest_eth1_voting_period_start_block(
            req.eth1_voting_period_start_timestamp
        )
        return GetDistanceResponse(
            distance=(eth1_voting_period_start_block_number - block.number)
        )

    async def _handle_new_logs(self) -> None:
        """
        Handle new blocks and the logs of them.
        """
        async for block in self._new_blocks():
            self._handle_block_data(block)
            logs = self._get_logs_from_block(block.number)
            self.logger.info(
                "Eth1 Monitor got new eth1 block: %s, number of logs contained in the block: %s",
                block,
                len(logs),
            )
            self._process_logs(logs, block.number)

    def _handle_get_deposit(self, req: GetDepositRequest) -> GetDepositResponse:
        """
        Handle requests for `get_deposit` from the event bus.
        """
        deposit = self._get_deposit(req.deposit_count, req.deposit_index)
        return GetDepositResponse.from_data(deposit)

    def _handle_get_eth1_data(self, req: GetEth1DataRequest) -> GetEth1DataResponse:
        """
        Handle requests for `get_eth1_data` from the event bus.
        """
        eth1_data = self._get_eth1_data(
            req.distance, req.eth1_voting_period_start_timestamp
        )
        return GetEth1DataResponse.from_data(eth1_data)

    def _get_eth1_data(
        self, distance: BlockNumber, eth1_voting_period_start_timestamp: Timestamp
    ) -> Eth1Data:
        """
        Return `Eth1Data` at `distance` relative to the eth1 block earlier and closest to the
        timestamp `eth1_voting_period_start_timestamp`.
        Ref: https://github.com/ethereum/eth2.0-specs/blob/61f2a0662ebcfb4c097360cc1835c5f01872705c/specs/validator/0_beacon-chain-validator.md#eth1-data  # noqa: E501

        First, we find the `eth1_block` whose timestamp is the largest timestamp which is smaller
        than `eth1_voting_period_start_timestamp`. Then, find the block `target_block` at number
        `eth1_block.number - distance`. Therefore, we can return `Eth1Data` according to the
        information of this block.
        """
        eth1_voting_period_start_block_number = self._get_closest_eth1_voting_period_start_block(
            eth1_voting_period_start_timestamp
        )
        target_block_number = BlockNumber(
            eth1_voting_period_start_block_number - distance
        )
        if target_block_number < 0:
            raise Eth1MonitorValidationError(
                f"target block number at `distance`={distance} is smaller than 0,",
                f"eth1_voting_period_start_block_number={eth1_voting_period_start_block_number}",
            )
        try:
            block = self._eth1_data_provider.get_block(target_block_number)
        except BlockNotFound:
            raise Eth1MonitorValidationError(
                f"Block does not exist for block number={target_block_number}"
            )
        block_hash = block.block_hash
        # `Eth1Data.deposit_count`: get the `deposit_count` corresponding to the block.
        contract_deposit_count = self._get_deposit_count_from_contract(
            target_block_number
        )
        if contract_deposit_count == 0:
            raise Eth1MonitorValidationError(
                f"failed to make `Eth1Data`: `deposit_count = 0` at block #{target_block_number}"
            )
        contract_deposit_root = self._get_deposit_root_from_contract(
            target_block_number
        )
        eth1_data = Eth1Data.create(
            deposit_root=contract_deposit_root,
            deposit_count=contract_deposit_count,
            block_hash=block_hash,
        )

        # If we have not processed any block
        if self._largest_block_timestamp is None:
            return eth1_data

        largest_block_number = self._block_timestamp_to_number[self._largest_block_timestamp]
        # If we have processed the target block number, validate deposit root.
        if largest_block_number >= target_block_number:
            # Verify that the deposit data in db and the deposit data in contract match
            deposit_data_in_range = self._db.get_deposit_data_range(
                0, contract_deposit_count
            )
            _, deposit_root = make_deposit_tree_and_root(deposit_data_in_range)
            if contract_deposit_root != deposit_root:
                raise DepositDataCorrupted(
                    "deposit root built locally mismatches the one in the contract on chain: "
                    f"contract_deposit_root={contract_deposit_root.hex()}, "
                    f"deposit_root={deposit_root.hex()}"
                )
        return eth1_data

    def _get_deposit(self, deposit_count: int, deposit_index: int) -> Deposit:
        """
        Return `Deposit` according to `deposit_count` and `deposit_index`.
        It should include the deposit data at the `deposit_index`, and the merkle proof of
        the corresponding merkle tree made from deposit data of size `deposit_count`.
        """
        if deposit_index >= deposit_count:
            raise Eth1MonitorValidationError(
                "`deposit_index` should be smaller than `deposit_count`: "
                f"deposit_index={deposit_index}, deposit_count={deposit_count}"
            )
        len_deposit_data = self.total_deposit_count
        if deposit_count <= 0 or deposit_count > len_deposit_data:
            raise Eth1MonitorValidationError(
                f"invalid `deposit_count`: deposit_count={deposit_count}"
            )
        if deposit_index < 0 or deposit_index >= len_deposit_data:
            raise Eth1MonitorValidationError(
                f"invalid `deposit_index`: deposit_index={deposit_index}"
            )
        deposit_data_in_range = self._db.get_deposit_data_range(0, deposit_count)
        tree, root = make_deposit_tree_and_root(deposit_data_in_range)
        return Deposit.create(
            proof=make_deposit_proof(deposit_data_in_range, tree, root, deposit_index),
            data=self._db.get_deposit_data(deposit_index),
        )

    async def _run_handle_request(
        self, event_type: Type[TRequest], event_handler: Callable[[TRequest], Any]
    ) -> None:
        async for req in self._event_bus.stream(event_type):
            try:
                resp = event_handler(req)
            except Exception as e:
                if isinstance(req, GetDistanceRequest):
                    await self._event_bus.broadcast(
                        req.expected_response_type()(None, e), req.broadcast_config()
                    )
                else:
                    await self._event_bus.broadcast(
                        req.expected_response_type()(None, None, e),
                        req.broadcast_config(),
                    )
            else:
                await self._event_bus.broadcast(resp, req.broadcast_config())

    async def _new_blocks(self) -> AsyncGenerator[Eth1Block, None]:
        """
        Keep polling latest blocks, and yield the blocks whose number is
        `latest_block.number - self._num_blocks_confirmed`.
        """
        while True:
            try:
                block = self._eth1_data_provider.get_block("latest")
            except BlockNotFound:
                raise Eth1MonitorValidationError("Fail to get latest block")
            target_block_number = BlockNumber(block.number - self._num_blocks_confirmed)
            from_block_number = self.highest_processed_block_number
            if target_block_number > from_block_number:
                # From `highest_processed_block_number` to `target_block_number`
                for block_number in range(
                    from_block_number + 1, target_block_number + 1
                ):
                    try:
                        block = self._eth1_data_provider.get_block(BlockNumber(block_number))
                    except BlockNotFound:
                        raise Eth1MonitorValidationError(
                            f"Block does not exist for block number={block_number}"
                        )
                    yield block
            await trio.sleep(self._polling_period)

    def _handle_block_data(self, block: Eth1Block) -> None:
        """
        Validate the block with information we already have, and put it
        in the proper data structures.
        """
        # Check timestamp.
        if len(self._block_timestamp_to_number) != 0:
            # Sanity check.
            if block.timestamp < self._largest_block_timestamp:
                raise Eth1MonitorValidationError(
                    "Later block with earlier timestamp: "
                    f"largest_timestamp={self._largest_block_timestamp}, "
                    f"block.timestamp={block.timestamp}"
                )
        self._block_timestamp_to_number[block.timestamp] = block.number
        self._largest_block_timestamp = block.timestamp

    def _get_logs_from_block(self, block_number: BlockNumber) -> Tuple[DepositLog, ...]:
        """
        Get the parsed logs inside the block with number `block_number`.
        """
        return self._eth1_data_provider.get_logs(block_number)

    def _process_logs(
        self, logs: Sequence[DepositLog], block_number: BlockNumber
    ) -> None:
        """
        Store deposit data from the log in database, and increase the corresponding block's
        `deposit_count`.
        """
        seq_deposit_data = tuple(
            DepositData.create(
                pubkey=log.pubkey,
                withdrawal_credentials=log.withdrawal_credentials,
                amount=log.amount,
                signature=log.signature,
            )
            for log in logs
        )
        self._db.add_deposit_data_batch(seq_deposit_data, block_number)

    def _get_closest_eth1_voting_period_start_block(
        self, timestamp: Timestamp
    ) -> BlockNumber:
        """
        Find the timestamp in `self._block_timestamp_to_number` which is the largest timestamp
        smaller than `timestamp`.
        Assume `self._block_timestamp_to_number` is in ascending order, the most naive way to find
        the timestamp is to traverse from the tail of `self._block_timestamp_to_number`.
        """
        # Compare with the largest recoreded block timestamp first before querying
        # for the latest block.
        # If timestamp larger than largest block timestamp, request block from eth1 provider.
        if self._largest_block_timestamp is None or timestamp > self._largest_block_timestamp:
            try:
                block = self._eth1_data_provider.get_block("latest")
            except BlockNotFound:
                raise Eth1MonitorValidationError("Fail to get latest block")
            if block.timestamp <= timestamp:
                return block.number
            else:
                block_number = block.number
                # Try the latest `self._num_blocks_confirmed` blocks until we give up
                for i in range(1, self._num_blocks_confirmed + 1):
                    block = self._eth1_data_provider.get_block(block_number - i)
                    if block.timestamp <= timestamp:
                        return block.number
                raise Eth1BlockNotFound(
                    "Can not find block with timestamp closest"
                    "to voting period start timestamp: %s",
                    timestamp,
                )
        else:
            # NOTE: It can be done by binary search with web3 queries.
            # Regarding the current block number is around `9000000`, not sure if it is worthwhile
            # to do it through web3 with `log(9000000, 2)` ~= 24 `getBlock` queries.
            # It's quite expensive compared to calculating it by the cached data
            # which involves 0 query.

            # Binary search for the right-most timestamp smaller than `timestamp`.
            all_timestamps = tuple(self._block_timestamp_to_number.keys())
            target_timestamp_index = bisect.bisect_right(all_timestamps, timestamp)
            # Though `index < 0` should never happen, check it for safety.
            if target_timestamp_index <= 0:
                raise Eth1BlockNotFound(
                    "Failed to find the closest eth1 voting period start block to "
                    f"timestamp {timestamp}"
                )
            else:
                # `bisect.bisect_right` returns the index we should insert `timestamp` into
                # `all_timestamps`, to make `all_timestamps` still in order. The element we are
                # looking for is actually `index - 1`
                index = target_timestamp_index - 1
                target_key = all_timestamps[index]
                return self._block_timestamp_to_number[target_key]

    def _get_deposit_count_from_contract(self, block_number: BlockNumber) -> int:
        """
        Get the accumulated deposit count from deposit contract with `get_deposit_count`
        at block `block_number`.
        """
        deposit_count_bytes = self._eth1_data_provider.get_deposit_count(block_number=block_number)
        return int.from_bytes(deposit_count_bytes, "little")

    def _get_deposit_root_from_contract(self, block_number: BlockNumber) -> Hash32:
        """
        Get the deposit root from deposit contract with `get_deposit_root`
        at block `block_number`.
        """
        return self._eth1_data_provider.get_deposit_root(block_number=block_number)
