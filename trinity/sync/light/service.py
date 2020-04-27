from abc import (
    ABC,
    abstractmethod,
)
import asyncio
from functools import (
    partial,
    wraps,
)
from typing import (
    Any,
    Callable,
    cast,
    List,
    FrozenSet,
    Optional,
    Tuple,
    Type,
)
import weakref

from async_service import Service, ServiceAPI

from async_lru import alru_cache

import rlp

from eth_typing import (
    Address as ETHAddress,
    Hash32,
)

from eth_hash.auto import keccak

from eth_utils import (
    encode_hex,
)

from trie import HexaryTrie
from trie.exceptions import BadTrieProof

from eth.exceptions import (
    BlockNotFound,
    HeaderNotFound,
)
from eth.rlp.accounts import Account
from eth.rlp.headers import BlockHeader
from eth.rlp.receipts import Receipt

from p2p.abc import CommandAPI
from p2p.exceptions import (
    BadLESResponse,
    NoConnectedPeers,
    NoEligiblePeers,
)
from p2p.commands import BaseCommand
from p2p.constants import (
    COMPLETION_TIMEOUT,
    MAX_REORG_DEPTH,
    MAX_REQUEST_ATTEMPTS,
    REPLY_TIMEOUT,
)
from p2p.disconnect import DisconnectReason
from p2p.peer import PeerSubscriber

from trinity.db.eth1.header import BaseAsyncHeaderDB
from trinity.protocol.les.peer import LESPeer, LESPeerPool
from trinity.protocol.les.commands import ProofsV1, ProofsV2
from trinity.rlp.block_body import BlockBody
from trinity._utils.logging import get_logger


def service_timeout(timeout: int) -> Callable[..., Any]:
    """
    Decorator to time out a method call.

    :param timeout: seconds to wait before raising a timeout exception

    :raise asyncio.TimeoutError: if the call is not complete before timeout seconds
    """
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        async def wrapped(service: ServiceAPI, *args: Any, **kwargs: Any) -> Any:
            return await asyncio.wait_for(
                func(service, *args, **kwargs),
                timeout=timeout,
            )
        return wrapped
    return decorator


class BaseLightPeerChain(ABC):

    @abstractmethod
    async def coro_get_block_header_by_hash(self, block_hash: Hash32) -> BlockHeader:
        ...

    @abstractmethod
    async def coro_get_block_body_by_hash(self, block_hash: Hash32) -> BlockBody:
        ...

    @abstractmethod
    async def coro_get_receipts(self, block_hash: Hash32) -> List[Receipt]:
        ...

    @abstractmethod
    async def coro_get_account(self, block_hash: Hash32, address: ETHAddress) -> Account:
        ...

    @abstractmethod
    async def coro_get_contract_code(self, block_hash: Hash32, address: ETHAddress) -> bytes:
        ...


class LightPeerChain(PeerSubscriber, Service, BaseLightPeerChain):
    reply_timeout = REPLY_TIMEOUT
    headerdb: BaseAsyncHeaderDB = None
    _pending_replies: "weakref.WeakValueDictionary[int, asyncio.Future[CommandAPI[Any]]]"

    def __init__(self, headerdb: BaseAsyncHeaderDB, peer_pool: LESPeerPool) -> None:
        PeerSubscriber.__init__(self)
        self.logger = get_logger('trinity.sync.light.LightPeerChain')
        self.headerdb = headerdb
        self.peer_pool = peer_pool
        self._pending_replies = weakref.WeakValueDictionary()

    # TODO: be more specific about what messages we want.
    subscription_msg_types: FrozenSet[Type[CommandAPI[Any]]] = frozenset({BaseCommand})

    # Here we only care about replies to our requests, ignoring most msgs (which are supposed
    # to be handled by the chain syncer), so our queue should never grow too much.
    msg_queue_maxsize = 500

    async def run(self) -> None:
        with self.subscribe(self.peer_pool):
            while self.manager.is_running:
                peer, cmd = await self.msg_queue.get()
                request_id = getattr(cmd.payload, 'request_id', None)
                # request_id can be None here because not all LES messages include one. For
                # instance, the Announce msg doesn't.
                if request_id is not None and request_id in self._pending_replies:
                    # This is the reply we are waiting for.  We avoid creating
                    # a local reference to the stored future to ensure that the
                    # weakref dictionary can clean up the entry.
                    self._pending_replies.pop(request_id).set_result(cmd)

    async def _wait_for_reply(self, request_id: int) -> CommandAPI[Any]:
        fut: 'asyncio.Future[CommandAPI[Any]]' = asyncio.Future()

        self._pending_replies[request_id] = fut
        return await asyncio.wait_for(fut, timeout=self.reply_timeout)

    @alru_cache(maxsize=1024, cache_exceptions=False)
    @service_timeout(COMPLETION_TIMEOUT)
    async def coro_get_block_header_by_hash(self, block_hash: Hash32) -> BlockHeader:
        """
        :param block_hash: hash of the header to retrieve

        :return: header returned by peer

        :raise NoEligiblePeers: if no peers are available to fulfill the request
        :raise asyncio.TimeoutError: if an individual request or the overall process times out
        """
        return await self._retry_on_bad_response(
            partial(self._get_block_header_by_hash, block_hash)
        )

    @alru_cache(maxsize=1024, cache_exceptions=False)
    @service_timeout(COMPLETION_TIMEOUT)
    async def coro_get_block_body_by_hash(self, block_hash: Hash32) -> BlockBody:
        peer = cast(LESPeer, self.peer_pool.highest_td_peer)
        self.logger.debug("Fetching block %s from %s", encode_hex(block_hash), peer)
        request_id = peer.les_api.send_get_block_bodies([block_hash])
        block_bodies = await self._wait_for_reply(request_id)
        if not block_bodies.payload.bodies:
            raise BlockNotFound(f"Peer {peer} has no block with hash {block_hash.hex()}")
        return block_bodies.payload.bodies[0]

    # TODO add a get_receipts() method to BaseChain API, and dispatch to this, as needed

    @alru_cache(maxsize=1024, cache_exceptions=False)
    @service_timeout(COMPLETION_TIMEOUT)
    async def coro_get_receipts(self, block_hash: Hash32) -> List[Receipt]:
        peer = cast(LESPeer, self.peer_pool.highest_td_peer)
        self.logger.debug("Fetching %s receipts from %s", encode_hex(block_hash), peer)
        request_id = peer.les_api.send_get_receipts((block_hash,))
        receipts = await self._wait_for_reply(request_id)
        if not receipts.payload.receipts:
            raise BlockNotFound(f"No block with hash {block_hash.hex()} found")
        return receipts.payload.receipts[0]

    # TODO implement AccountDB exceptions that provide the info needed to
    # request accounts and code (and storage?)

    @alru_cache(maxsize=1024, cache_exceptions=False)
    @service_timeout(COMPLETION_TIMEOUT)
    async def coro_get_account(self, block_hash: Hash32, address: ETHAddress) -> Account:
        return await self._retry_on_bad_response(
            partial(self._get_account_from_peer, block_hash, address)
        )

    async def _get_account_from_peer(
            self,
            block_hash: Hash32,
            address: ETHAddress,
            peer: LESPeer) -> Account:
        state_key = keccak(address)
        proof = await self._get_proof(peer, block_hash, state_key=state_key, storage_key=None)
        header = await self._get_block_header_by_hash(block_hash, peer)
        try:
            rlp_account = HexaryTrie.get_from_proof(header.state_root, state_key, proof)
        except BadTrieProof as exc:
            raise BadLESResponse(
                f"Peer {peer} returned an invalid proof for account {encode_hex(address)} "
                f"at block {encode_hex(block_hash)}"
            ) from exc
        return rlp.decode(rlp_account, sedes=Account)

    @alru_cache(maxsize=1024, cache_exceptions=False)
    @service_timeout(COMPLETION_TIMEOUT)
    async def coro_get_contract_code(self, block_hash: Hash32, address: ETHAddress) -> bytes:
        """
        :param block_hash: find code as of the block with block_hash
        :param address: which contract to look up

        :return: bytecode of the contract, ``b''`` if no code is set

        :raise NoEligiblePeers: if no peers are available to fulfill the request
        :raise asyncio.TimeoutError: if an individual request or the overall process times out
        """
        # get account for later verification, and
        # to confirm that our highest total difficulty peer has the info
        try:
            account = await self.coro_get_account(block_hash, address)
        except HeaderNotFound as exc:
            raise NoEligiblePeers(
                f"Our best peer does not have header {block_hash.hex()}"
            ) from exc

        code_hash = account.code_hash

        return await self._retry_on_bad_response(
            partial(self._get_contract_code_from_peer, block_hash, address, code_hash)
        )

    async def _get_contract_code_from_peer(
            self,
            block_hash: Hash32,
            address: ETHAddress,
            code_hash: Hash32,
            peer: LESPeer) -> bytes:
        """
        A single attempt to get the contract code from the given peer

        :raise BadLESResponse: if the peer replies with contract code that does not match the
            account's code hash
        """
        # request contract code
        request_id = peer.les_api.send_get_contract_code(block_hash, keccak(address))
        contract_codes = await self._wait_for_reply(request_id)

        if not contract_codes.payload.codes:
            bytecode = b''
        else:
            bytecode = contract_codes.payload.codes[0]

        # validate bytecode against a proven account
        if code_hash == keccak(bytecode):
            return bytecode
        elif bytecode == b'':
            await self._raise_for_empty_code(block_hash, address, code_hash, peer)
            # The following is added for mypy linting:
            raise RuntimeError("Unreachable, _raise_for_empty_code must raise its own exception")
        else:
            # a bad-acting peer sent an invalid non-empty bytecode
            raise BadLESResponse(
                "Peer {peer} sent code {encode_hex(bytecode)} that did not match "
                f"hash {encode_hex(code_hash)} in account {encode_hex(address)}"
            )

    async def _raise_for_empty_code(
            self,
            block_hash: Hash32,
            address: ETHAddress,
            code_hash: Hash32,
            peer: LESPeer) -> None:
        """
        A peer might return b'' if it doesn't have the block at the requested header,
        or it might maliciously return b'' when the code is non-empty. This method tries to tell the
        difference.

        This method MUST raise an exception, it's trying to determine the appropriate one.

        :raise BadLESResponse: if peer seems to be maliciously responding with invalid empty code
        :raise NoEligiblePeers: if peer might simply not have the code available
        """
        try:
            header = await self._get_block_header_by_hash(block_hash, peer)
        except HeaderNotFound:
            # We presume that the current peer is the best peer. Because
            # our best peer doesn't have the header we want, there are no eligible peers.
            raise NoEligiblePeers(f"Our best peer does not have the header {block_hash.hex()}")

        head_number = peer.head_info.head_number
        if head_number - header.block_number > MAX_REORG_DEPTH:
            # The peer claims to be far ahead of the header we requested
            if await self.headerdb.coro_get_canonical_block_hash(header.block_number) == block_hash:
                # Our node believes that the header at the reference hash is canonical,
                # so treat the peer as malicious
                raise BadLESResponse(
                    f"Peer {peer} sent empty code that did not match hash {encode_hex(code_hash)} "
                    f"in account {encode_hex(address)}"
                )
            else:
                # our header isn't canonical, so treat the empty response as missing data
                raise NoEligiblePeers(
                    f"Our best peer does not have the non-canonical header {block_hash.hex()}"
                )
        elif head_number - header.block_number < 0:
            # The peer claims to be behind the header we requested, but somehow served it to us.
            # Odd, it might be a race condition. Treat as if there are no eligible peers for now.
            raise NoEligiblePeers(f"Our best peer's head does include header {block_hash.hex()}")
        else:
            # The peer is ahead of the current block header, but only by a bit. It might be on
            # an uncle, or we might be. So we can't tell the difference between missing and
            # malicious. We don't want to aggressively drop this peer, so treat the code as missing.
            raise NoEligiblePeers(
                f"Peer {peer} claims to be ahead of {header}, but "
                f"returned empty code with hash {code_hash.hex()}. "
                f"It is on number {head_number}, maybe an uncle. Retry with an older block hash."
            )

    async def _get_block_header_by_hash(self, block_hash: Hash32, peer: LESPeer) -> BlockHeader:
        """
        A single attempt to get the block header from the given peer.

        :raise BadLESResponse: if the peer replies with a header that has a different hash
        """
        self.logger.debug("Fetching header %s from %s", encode_hex(block_hash), peer)
        max_headers = 1

        # TODO: Figure out why mypy thinks the first parameter to `get_block_headers`
        # should be of type `int`
        headers = await peer.chain_api.get_block_headers(
            block_hash,
            max_headers,
            skip=0,
            reverse=False,
        )
        if not headers:
            raise HeaderNotFound(f"Peer {peer} has no block with hash {block_hash.hex()}")
        header = headers[0]
        if header.hash != block_hash:
            raise BadLESResponse(
                f"Received header hash ({header.hex_hash}) does not "
                f"match what we requested ({encode_hex(block_hash)})"
            )
        return header

    async def _get_proof(self,
                         peer: LESPeer,
                         block_hash: Hash32,
                         state_key: Hash32,
                         storage_key: Optional[Hash32],
                         from_level: int = 0) -> Tuple[bytes, ...]:
        request_id = peer.les_api.send_get_proof(block_hash, state_key, storage_key, from_level)
        proofs = await self._wait_for_reply(request_id)
        if isinstance(proofs, ProofsV1):
            if proofs.payload.proofs:
                return proofs.payload.proofs[0]
            else:
                return ()
        elif isinstance(proofs, ProofsV2):
            return proofs.payload.proof
        else:
            raise Exception("Unreachable")

    async def _retry_on_bad_response(self, make_request_to_peer: Callable[[LESPeer], Any]) -> Any:
        """
        Make a call to a peer. If it behaves badly, drop it and retry with a different peer.

        :param make_request_to_peer: an abstract call to a peer that may raise a BadLESResponse

        :raise NoEligiblePeers: if no peers are available to fulfill the request
        :raise asyncio.TimeoutError: if an individual request or the overall process times out
        """
        for _ in range(MAX_REQUEST_ATTEMPTS):
            try:
                peer = cast(LESPeer, self.peer_pool.highest_td_peer)
            except NoConnectedPeers as exc:
                raise NoEligiblePeers() from exc

            try:
                return await make_request_to_peer(peer)
            except BadLESResponse as exc:
                self.logger.warning("Disconnecting from peer, because: %s", exc)
                await peer.disconnect(DisconnectReason.SUBPROTOCOL_ERROR)
                # reattempt after removing this peer from our pool

        raise asyncio.TimeoutError(
            f"Could not complete peer request in {MAX_REQUEST_ATTEMPTS} attempts"
        )
