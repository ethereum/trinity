from abc import ABC, abstractmethod

from typing import Tuple

from eth_typing import Hash32

from eth_utils import encode_hex

from eth.abc import DatabaseAPI

import rlp

from trinity.exceptions import WitnessHashesUnavailable
from trinity._utils.async_dispatch import async_method


class AsyncWitnessDataBaseAPI(ABC):
    @abstractmethod
    async def coro_get_witness_hashes(self, block_hash: Hash32) -> Tuple[Hash32, ...]:
        ...

    @abstractmethod
    async def coro_persist_witness_hashes(
        self,
        block_hash: Hash32,
        witness_hashes: Tuple[Hash32, ...],
    ) -> None:
        ...


class AsyncWitnessDB(AsyncWitnessDataBaseAPI):
    _recent_blocks_with_witnesses_lookup_key = b'recent-blocks-with-witness-hashes'
    _max_witness_history = 256

    def __init__(self, db: DatabaseAPI) -> None:
        self.db = db

    def _make_block_witness_hashes_lookup_key(self, block_hash: Hash32) -> bytes:
        return b'block-witness-hashes:%s' % block_hash

    def _get_recent_blocks_with_witnesses(self) -> Tuple[Hash32, ...]:
        return tuple(rlp.decode(self.db[self._recent_blocks_with_witnesses_lookup_key]))

    def get_witness_hashes(self, block_hash: Hash32) -> Tuple[Hash32, ...]:
        try:
            return tuple(
                rlp.decode(self.db[self._make_block_witness_hashes_lookup_key(block_hash)]))
        except KeyError:
            raise WitnessHashesUnavailable("No witness hashes for block %s", encode_hex(block_hash))

    def persist_witness_hashes(
            self, block_hash: Hash32, witness_hashes: Tuple[Hash32, ...]) -> None:
        try:
            recent_blocks_with_witnesses = list(self._get_recent_blocks_with_witnesses())
        except KeyError:
            recent_blocks_with_witnesses = []

        # Add in the new block, if it's not already present
        if block_hash not in recent_blocks_with_witnesses:
            recent_blocks_with_witnesses.append(block_hash)

        # Flush out old witnesses
        while len(recent_blocks_with_witnesses) > self._max_witness_history:
            oldest_block_witness = recent_blocks_with_witnesses.pop(0)
            del self.db[self._make_block_witness_hashes_lookup_key(oldest_block_witness)]

        # Store new reference to existing witness
        self.db[self._recent_blocks_with_witnesses_lookup_key] = rlp.encode(
            recent_blocks_with_witnesses)

        try:
            # Note: if this call is converted to async, watch out for the race
            #   condition of two persists that interleave. It would be
            #   possible for one to overwrite the other. For now, the synchronous
            #   approach means that that isn't a concern.
            existing_hashes = self.get_witness_hashes(block_hash)
        except WitnessHashesUnavailable:
            existing_hashes = ()

        block_hashes_key = self._make_block_witness_hashes_lookup_key(block_hash)
        combined_hashes = tuple(set(existing_hashes).union(witness_hashes))
        self.db[block_hashes_key] = rlp.encode(combined_hashes)

    coro_get_witness_hashes = async_method(get_witness_hashes)
    coro_persist_witness_hashes = async_method(persist_witness_hashes)
