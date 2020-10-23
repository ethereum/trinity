from abc import ABC, abstractmethod

from typing import Tuple

from eth_typing import Hash32

from eth.abc import AtomicDatabaseAPI

import rlp

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

    def __init__(self, db: AtomicDatabaseAPI) -> None:
        self.db = db

    def _make_block_witness_hashes_lookup_key(self, block_hash: Hash32) -> bytes:
        return b'block-witness-hashes:%s' % block_hash

    def _get_recent_blocks_with_witnesses(self) -> Tuple[Hash32, ...]:
        return tuple(rlp.decode(self.db[self._recent_blocks_with_witnesses_lookup_key]))

    def get_witness_hashes(self, block_hash: Hash32) -> Tuple[Hash32, ...]:
        return tuple(rlp.decode(self.db[self._make_block_witness_hashes_lookup_key(block_hash)]))

    def persist_witness_hashes(
            self, block_hash: Hash32, witness_hashes: Tuple[Hash32, ...]) -> None:
        try:
            recent_blocks_with_witnesses = list(self._get_recent_blocks_with_witnesses())
        except KeyError:
            recent_blocks_with_witnesses = []

        if len(recent_blocks_with_witnesses) == self._max_witness_history:
            oldest_block_witness = recent_blocks_with_witnesses.pop(0)
            del self.db[self._make_block_witness_hashes_lookup_key(oldest_block_witness)]

        recent_blocks_with_witnesses.append(block_hash)
        self.db[self._recent_blocks_with_witnesses_lookup_key] = rlp.encode(
            recent_blocks_with_witnesses)
        self.db[self._make_block_witness_hashes_lookup_key(block_hash)] = rlp.encode(witness_hashes)

    coro_get_witness_hashes = async_method(get_witness_hashes)
    coro_persist_witness_hashes = async_method(persist_witness_hashes)
