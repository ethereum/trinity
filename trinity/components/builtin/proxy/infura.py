import time
from typing import TypedDict, Tuple, Iterable, Union

from eth.abc import BlockHeaderAPI
from eth.exceptions import HeaderNotFound
from eth.rlp.headers import BlockHeader
from eth_typing import HexAddress, Hash32, BlockNumber
from eth_utils import to_canonical_address, big_endian_to_int
from hexbytes import HexBytes
from lru import LRU
from web3.exceptions import BlockNotFound

from trinity.db.eth1.header import AsyncHeaderDB


class Web3Block(TypedDict):
    difficulty: int
    number: int
    gasLimit: int
    timestamp: int
    coinbase: HexAddress
    parentHash: HexBytes
    sha3Uncles: HexBytes
    stateRoot: HexBytes
    transactionsRoot: HexBytes
    receiptsRoot: HexBytes
    logsBloom: HexBytes
    gasUsed: int
    extraData: HexBytes
    mixHash: HexBytes
    nonce: HexBytes

    hash: HexBytes
    totalDifficulty: int


def w3_block_to_canonical_header(w3_block: Web3Block) -> BlockHeaderAPI:
    return BlockHeader(
        difficulty=w3_block["difficulty"],
        block_number=w3_block["number"],
        gas_limit=w3_block["gasLimit"],
        timestamp=w3_block["timestamp"],
        coinbase=to_canonical_address(w3_block["miner"]),
        parent_hash=Hash32(w3_block["parentHash"]),
        uncles_hash=Hash32(w3_block["sha3Uncles"]),
        state_root=Hash32(w3_block["stateRoot"]),
        transaction_root=Hash32(w3_block["transactionsRoot"]),
        receipt_root=Hash32(w3_block["receiptsRoot"]),
        bloom=big_endian_to_int(bytes(w3_block["logsBloom"])),
        gas_used=w3_block["gasUsed"],
        extra_data=bytes(w3_block["extraData"]),
        mix_hash=Hash32(w3_block["mixHash"]),
        nonce=bytes(w3_block["nonce"]),
    )


class InfuraHeaderDB(AsyncHeaderDB):
    def __init__(self) -> None:
        from web3.auto.infura import w3
        self._w3 = w3
        self._block_cache = LRU(1024)

        self._latest_block_at = 0

    def _get_w3_block(self, identifier: Union[BlockNumber, Hash32]) -> BlockHeaderAPI:
        try:
            if identifier == 'latest':
                if time.time() - self._latest_block_at > 10:
                    self._latest_block = self._w3.eth.get_block('latest')
                    self._latest_block_at = time.time()
                return self._latest_block
            else:
                if identifier not in self._block_cache:
                    self._block_cache[identifier] = self._w3.eth.get_block(identifier)
                return self._block_cache[identifier]
        except BlockNotFound as err:
            raise HeaderNotFound(str(err)) from err

    async def coro_get_canonical_block_hash(self, block_number: BlockNumber) -> Hash32:
        w3_block = self._get_w3_block(block_number)
        return Hash32(w3_block['hash'])

    async def coro_get_canonical_block_header_by_number(self, block_number: BlockNumber) -> BlockHeaderAPI:  # noqa: E501
        w3_block = self._get_w3_block(block_number)
        return w3_block_to_canonical_header(w3_block)

    async def coro_get_canonical_head(self) -> BlockHeaderAPI:
        w3_block = self._get_w3_block('latest')
        return w3_block_to_canonical_header(w3_block)

    #
    # Header API
    #
    async def coro_get_block_header_by_hash(self, block_hash: Hash32) -> BlockHeaderAPI:
        w3_block = self._get_w3_block(block_hash)
        return w3_block_to_canonical_header(w3_block)

    async def coro_get_score(self, block_hash: Hash32) -> int:
        w3_block = self._get_w3_block(block_hash)
        return w3_block['totalDifficulty']

    async def coro_header_exists(self, block_hash: Hash32) -> bool:
        try:
            self._get_w3_block(block_hash)
        except HeaderNotFound:
            return False
        else:
            return True

    async def coro_persist_header(
        self,
        header: BlockHeaderAPI,
    ) -> Tuple[Tuple[BlockHeaderAPI, ...], Tuple[BlockHeaderAPI, ...]]:
        pass

    async def coro_persist_checkpoint_header(self, header: BlockHeaderAPI, score: int) -> None:
        pass

    async def coro_persist_header_chain(
        self,
        headers: Iterable[BlockHeaderAPI],
        genesis_parent_hash: Hash32 = None
    ) -> Tuple[Tuple[BlockHeaderAPI, ...], Tuple[BlockHeaderAPI, ...]]:
        pass
