import binascii
import itertools
import logging
import sys
from typing import Iterator, Tuple, Type

import rlp

from eth.abc import VirtualMachineAPI
from eth.vm.forks.homestead import HomesteadVM

from eth_typing import BlockNumber, Hash32

from eth_utils import encode_hex, to_tuple

from p2p.discv5.enr import ENR
from p2p.exceptions import MalformedMessage

from trinity.exceptions import ENRMissingForkID, RemoteChainIsStale, LocalChainIncompatibleOrStale


class ForkID(rlp.Serializable):
    fields = [('hash', rlp.sedes.binary.fixed_length(4)), ('next', rlp.sedes.big_endian_int)]

    def __init__(self, hash: bytes, next: BlockNumber) -> None:
        # We often instantiate ForkIDs directly (rather than via rlp.decode()), so we validate the
        # hash length here as well to catch mistakes before we attempt to serialize.
        if len(hash) != 4:
            raise ValueError("Hash {hash!r} length is not 4")
        super().__init__(hash, next)

    def __repr__(self) -> str:
        return f"ForkID(hash={encode_hex(self.hash)}, next={self.next})"


@to_tuple
def extract_fork_blocks(
        vm_configuration: Tuple[
            Tuple[BlockNumber, Type[VirtualMachineAPI]], ...]
) -> Iterator[BlockNumber]:
    for block, vm in vm_configuration:
        if block == 0:
            continue
        yield block
        if issubclass(vm, HomesteadVM) and vm.support_dao_fork:
            yield vm.get_dao_fork_block_number()


def make_forkid(
        genesis_hash: Hash32,
        current_head: BlockNumber,
        fork_blocks: Tuple[BlockNumber, ...],
) -> ForkID:
    pre_hash_bytes = bytes(genesis_hash)
    next_fork = 0
    for block_number in fork_blocks:
        if block_number > current_head:
            next_fork = block_number
            break
        # As defined in the spec, we must checksum the 64-bit representation of the block number.
        pre_hash_bytes += block_number.to_bytes(8, 'big')
    checksum = _crc_to_bytes(binascii.crc32(pre_hash_bytes))
    return ForkID(hash=checksum, next=BlockNumber(next_fork))


def validate_forkid(
        forkid: ForkID,
        genesis_hash: Hash32,
        head: BlockNumber,
        fork_blocks: Tuple[BlockNumber, ...],
) -> None:
    """
    Validate the given ForkID against our current state.

    Validation rules are described at
      https://github.com/ethereum/EIPs/blob/master/EIPS/eip-2124.md#validation-rules
    """

    fork_blocks_list = list(fork_blocks)
    checksums = [binascii.crc32(genesis_hash)]
    for block_number in fork_blocks_list:
        block_number_as_bytes = block_number.to_bytes(8, 'big')
        checksums.append(binascii.crc32(block_number_as_bytes, checksums[-1]))

    fork_blocks_list.append(BlockNumber(sys.maxsize))
    for i, block_number in enumerate(fork_blocks_list):
        if head > block_number:
            # Our head is beyound this fork, so continue. We have a dummy fork block as the last
            # item in fork_blocks to ensure this check fails eventually.
            continue

        # Found the first unpassed fork block, check if our current state matches
        # the remote checksum (rule #1).
        if _crc_to_bytes(checksums[i]) == forkid.hash:
            # Fork checksum matched, check if a remote future fork block already passed
            # locally without the local node being aware of it (rule #1a).
            if forkid.next > 0 and head >= forkid.next:
                raise LocalChainIncompatibleOrStale("rule 1a")
            # Haven't passed locally a remote-only fork, accept the connection (rule #1b).
            return

        # We're in different forks currently, check if the remote checksum is a subset of our
        # local forks (rule #2).
        for b, checksum in itertools.zip_longest(fork_blocks_list[:i], checksums[:i]):
            if _crc_to_bytes(checksum) == forkid.hash:
                # Remote checksum is a subset, validate based on the announced next fork
                if b != forkid.next:
                    raise RemoteChainIsStale()
                return

        # Remote chain is not a subset of our local one, check if it's a superset by
        # any chance, signalling that we're simply out of sync (rule #3).
        for checksum in checksums[i:]:
            if _crc_to_bytes(checksum) == forkid.hash:
                # Remote checksum is a superset, ignore upcoming forks
                return

        # No exact, subset or superset match. We are on differing chains, reject.
        raise LocalChainIncompatibleOrStale("different chains")

    # Something is very wrong if we get here, but better to accept than reject.
    logging.getLogger('p2p').error("Impossible forkid validation for %s", forkid)


def extract_forkid(enr: ENR) -> ForkID:
    try:
        eth_cap = enr[b'eth']
    except KeyError:
        raise ENRMissingForkID()

    try:
        [forkid] = rlp.sedes.List([ForkID]).deserialize(eth_cap)
        return forkid
    except rlp.exceptions.ListDeserializationError:
        raise MalformedMessage("Unable to extract ForkID from {eth_cap}")


def _crc_to_bytes(crc: int) -> bytes:
    # ForkID checksums must be 4 bytes
    return crc.to_bytes(4, 'big')
