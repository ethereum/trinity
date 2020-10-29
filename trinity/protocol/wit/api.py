from typing import Any, Tuple
from trinity._utils.logging import get_logger

from cached_property import cached_property

from eth_typing import Hash32

from p2p.exchange import (
    BaseExchange,
    ExchangeAPI,
    ExchangeLogic,
    ValidatorAPI,
)
from p2p.exchange.normalizers import DefaultNormalizer
from p2p.exchange.tracker import BasePerformanceTracker
from p2p.logic import Application
from p2p.qualifiers import HasProtocol

from trinity._utils.les import (
    gen_request_id,
)
from trinity.protocol.common.validators import (
    match_payload_request_id,
)
from .commands import (
    BlockWitnessHashes,
    BlockWitnessHashesPayload,
    GetBlockWitnessHashes,
    GetBlockWitnessHashesPayload,
)
from .proto import WitnessProtocol


class GetBlockWitnessHashesTracker(
        BasePerformanceTracker[GetBlockWitnessHashes, BlockWitnessHashesPayload]):
    def _get_request_size(self, request: GetBlockWitnessHashes) -> int:
        # We can never know how many witness hashes to expect for a given block.
        return None

    def _get_result_size(self, result: BlockWitnessHashesPayload) -> int:
        return len(result.node_hashes)

    def _get_result_item_count(self, result: BlockWitnessHashesPayload) -> int:
        return len(result.node_hashes)


class GetBlockWitnessHashesValidator(ValidatorAPI[BlockWitnessHashesPayload]):

    def validate_result(self, response: BlockWitnessHashesPayload) -> None:
        # Nothing we can do here as even an empty result is valid.
        pass


class BlockWitnessHashesExchange(
        BaseExchange[GetBlockWitnessHashes, BlockWitnessHashes, BlockWitnessHashesPayload]):

    _request_command_type = GetBlockWitnessHashes
    _response_command_type = BlockWitnessHashes
    _normalizer = DefaultNormalizer(BlockWitnessHashes, BlockWitnessHashesPayload)
    tracker_class = GetBlockWitnessHashesTracker

    def __init__(self) -> None:
        super().__init__()
        self.logger = get_logger('trinity.protocol.wit.api.BlockWitnessHashesExchange')

    async def __call__(  # type: ignore
            self, block_hash: Hash32, timeout: float = None) -> Tuple[Hash32, ...]:
        validator = GetBlockWitnessHashesValidator()
        request = GetBlockWitnessHashes(GetBlockWitnessHashesPayload(gen_request_id(), block_hash))
        result = await self.get_result(
            request,
            self._normalizer,
            validator,
            match_payload_request_id,
            timeout,
        )
        return result.node_hashes


class WitnessAPI(Application):
    name = 'wit'
    qualifier = HasProtocol(WitnessProtocol)

    def __init__(self) -> None:
        self.logger = get_logger('trinity.protocol.wit.api.WitnessAPI')
        self.get_block_witness_hashes = BlockWitnessHashesExchange()
        self.add_child_behavior(ExchangeLogic(self.get_block_witness_hashes).as_behavior())

    @cached_property
    def protocol(self) -> WitnessProtocol:
        return self.connection.get_protocol_by_type(WitnessProtocol)

    @cached_property
    def exchanges(self) -> Tuple[ExchangeAPI[Any, Any, Any], ...]:
        return (self.get_block_witness_hashes,)

    def get_extra_stats(self) -> Tuple[str, ...]:
        return tuple(
            f"{exchange.get_response_cmd_type()}: {exchange.tracker.get_stats()}"
            for exchange in self.exchanges
        )
