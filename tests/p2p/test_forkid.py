import sys

import pytest

import rlp

from eth_utils import to_bytes

from eth.chains.mainnet import MAINNET_VM_CONFIGURATION
from eth.chains.ropsten import ROPSTEN_VM_CONFIGURATION

from p2p.exceptions import RemoteChainIsStale, LocalChainIncompatibleOrStale
from p2p.forkid import ForkID, make_forkid, validate_forkid

MAINNET_GENESIS_HASH = to_bytes(
    hexstr='0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3')
ROPSTEN_GENESIS_HASH = to_bytes(
    hexstr='0x41941023680923e0fe4d74a34bdac8141f2540e3ae90623718e47d66d1ca4a2d')


@pytest.mark.parametrize(
    'head,expected_forkid',
    [
        (0, ForkID(hash=to_bytes(hexstr='0xfc64ec04'), next=1150000)),        # Unsynced
        (1149999, ForkID(hash=to_bytes(hexstr='0xfc64ec04'), next=1150000)),  # Last Frontier
        (1150000, ForkID(hash=to_bytes(hexstr='0x97c2c34c'), next=1920000)),  # First Homestead
        (1919999, ForkID(hash=to_bytes(hexstr='0x97c2c34c'), next=1920000)),  # Last Homestead
        (1920000, ForkID(hash=to_bytes(hexstr='0x91d1f948'), next=2463000)),  # First DAO block
        (2462999, ForkID(hash=to_bytes(hexstr='0x91d1f948'), next=2463000)),  # Last DAO block
        (2463000, ForkID(hash=to_bytes(hexstr='0x7a64da13'), next=2675000)),  # First Tangerine
        (2674999, ForkID(hash=to_bytes(hexstr='0x7a64da13'), next=2675000)),  # Last Tangerine
        (2675000, ForkID(hash=to_bytes(hexstr='0x3edd5b10'), next=4370000)),  # First Spurious
        (4369999, ForkID(hash=to_bytes(hexstr='0x3edd5b10'), next=4370000)),  # Last Spurious
        (4370000, ForkID(hash=to_bytes(hexstr='0xa00bc324'), next=7280000)),  # First Byzantium
        (7279999, ForkID(hash=to_bytes(hexstr='0xa00bc324'), next=7280000)),  # Last Byzantium
        # First and last Constantinople, first Petersburg block
        (7280000, ForkID(hash=to_bytes(hexstr='0x668db0af'), next=9069000)),
        (9068999, ForkID(hash=to_bytes(hexstr='0x668db0af'), next=9069000)),  # Last Petersburg
        # First Istanbul and first Muir Glacier block
        (9069000, ForkID(hash=to_bytes(hexstr='0x879d6e30'), next=9200000)),
        # Last Istanbul and first Muir Glacier block
        (9199999, ForkID(hash=to_bytes(hexstr='0x879d6e30'), next=9200000)),
        (9200000, ForkID(hash=to_bytes(hexstr='0xe029e991'), next=0)),   # First Muir Glacier block
        (10000000, ForkID(hash=to_bytes(hexstr='0xe029e991'), next=0)),  # Future Muir Glacier block
    ]
)
def test_mainnet_forkids(head, expected_forkid):
    _test_make_forkid(MAINNET_VM_CONFIGURATION, MAINNET_GENESIS_HASH, head, expected_forkid)


@pytest.mark.parametrize(
    'head,expected_forkid',
    [
        # Unsynced, last Frontier, Homestead and first Tangerine block
        (0, ForkID(hash=to_bytes(hexstr='0x30c7ddbc'), next=10)),
        (9, ForkID(hash=to_bytes(hexstr='0x30c7ddbc'), next=10)),  # Last Tangerine block
        (10, ForkID(hash=to_bytes(hexstr='0x63760190'), next=1700000)),  # First Spurious block
        (1699999, ForkID(hash=to_bytes(hexstr='0x63760190'), next=1700000)),  # Last Spurious block
        (1700000, ForkID(hash=to_bytes(hexstr='0x3ea159c7'), next=4230000)),  # First Byzantium
        (4229999, ForkID(hash=to_bytes(hexstr='0x3ea159c7'), next=4230000)),  # Last Byzantium
        (4230000, ForkID(hash=to_bytes(hexstr='0x97b544f3'), next=4939394)),  # First Constantinople
        (4939393, ForkID(hash=to_bytes(hexstr='0x97b544f3'), next=4939394)),  # Last Constantinople
        (4939394, ForkID(hash=to_bytes(hexstr='0xd6e2149b'), next=6485846)),  # First Petersburg
        (6485845, ForkID(hash=to_bytes(hexstr='0xd6e2149b'), next=6485846)),  # Last Petersburg
        (6485846, ForkID(hash=to_bytes(hexstr='0x4bc66396'), next=7117117)),  # First Istanbul
        (7117116, ForkID(hash=to_bytes(hexstr='0x4bc66396'), next=7117117)),  # Last Istanbul block
        (7117117, ForkID(hash=to_bytes(hexstr='0x6727ef90'), next=0)),  # First Muir Glacier block
        (7500000, ForkID(hash=to_bytes(hexstr='0x6727ef90'), next=0)),  # Future
    ]
)
def test_ropsten_forkids(head, expected_forkid):
    _test_make_forkid(ROPSTEN_VM_CONFIGURATION, ROPSTEN_GENESIS_HASH, head, expected_forkid)


def _test_make_forkid(vm_config, genesis_hash, head, expected_forkid):
    forkid = make_forkid(genesis_hash, head, vm_config)
    assert forkid.hash == expected_forkid.hash
    assert forkid.next == expected_forkid.next


def test_forkid():
    forkid = ForkID(hash=b'\xe0)\xe9\x91', next=999)
    assert forkid.hash == b'\xe0)\xe9\x91'
    assert forkid.next == 999

    # A hash with length diffrent than 4 is not allowed.
    with pytest.raises(ValueError):
        forkid = ForkID(hash=b'\x00\x00\x00\x02Q\xc0', next=0)
    with pytest.raises(ValueError):
        forkid = ForkID(hash=b'\x02Q\xc0', next=0)


@pytest.mark.parametrize(
    'local_head,remote_forkid,expected_error',
    [
        # Local is mainnet Petersburg, remote announces the same. No future fork is announced.
        (7987396, ForkID(hash=to_bytes(hexstr='0x668db0af'), next=0), None),

        # Local is mainnet Petersburg, remote announces the same. Remote also announces a next fork
        # at block 0xffffffff, but that is uncertain.
        (7987396, ForkID(hash=to_bytes(hexstr='0x668db0af'), next=sys.maxsize), None),

        # Local is mainnet currently in Byzantium only (so it's aware of Petersburg), remote
        # announces also Byzantium, but it's not yet aware of Petersburg (e.g. non updated node
        # before the fork).  In this case we don't know if Petersburg passed yet or not.
        (7279999, ForkID(hash=to_bytes(hexstr='0xa00bc324'), next=0), None),

        # Local is mainnet currently in Byzantium only (so it's aware of Petersburg), remote
        # announces also Byzantium, and it's also aware of Petersburg (e.g. updated node before
        # the fork). We don't know if Petersburg passed yet (will pass) or not.
        (7279999, ForkID(hash=to_bytes(hexstr='0xa00bc324'), next=7280000), None),

        # Local is mainnet currently in Byzantium only (so it's aware of Petersburg), remote
        # announces also Byzantium, and it's also aware of some random fork (e.g. misconfigured
        # Petersburg). As neither forks passed at neither nodes, they may mismatch, but we still
        # connect for now.
        (7279999, ForkID(hash=to_bytes(hexstr='0xa00bc324'), next=sys.maxsize), None),

        # Local is mainnet Petersburg, remote announces Byzantium + knowledge about Petersburg.
        # Remote is simply out of sync, accept.
        (7987396, ForkID(hash=to_bytes(hexstr='0xa00bc324'), next=7280000), None),

        # Local is mainnet Petersburg, remote announces Spurious + knowledge about Byzantium.
        # Remote is definitely out of sync. It may or may not need the Petersburg update, we don't
        # know yet.
        (7987396, ForkID(hash=to_bytes(hexstr='0x3edd5b10'), next=4370000), None),

        # Local is mainnet Byzantium, remote announces Petersburg. Local is out of sync, accept.
        (7279999, ForkID(hash=to_bytes(hexstr='0x668db0af'), next=0), None),

        # Local is mainnet Spurious, remote announces Byzantium, but is not aware of Petersburg.
        # Local out of sync. Local also knows about a future fork, but that is uncertain yet.
        (4369999, ForkID(hash=to_bytes(hexstr='0xa00bc324'), next=0), None),

        # Local is mainnet Petersburg. remote announces Byzantium but is not aware of further forks.
        # Remote needs software update.
        (7987396, ForkID(hash=to_bytes(hexstr='0xa00bc324'), next=0), RemoteChainIsStale),

        # Local is mainnet Petersburg, and isn't aware of more forks. Remote announces Petersburg +
        # 0xffffffff. Local needs software update, reject.
        (7987396,
         ForkID(hash=to_bytes(hexstr='0x5cddc0e1'), next=0),
         LocalChainIncompatibleOrStale),

        # Local is mainnet Byzantium, and is aware of Petersburg. Remote announces Petersburg +
        # 0xffffffff. Local needs software update, reject.
        (7279999,
         ForkID(hash=to_bytes(hexstr='0x5cddc0e1'), next=0),
         LocalChainIncompatibleOrStale),

        # Local is mainnet Petersburg, remote is Rinkeby Petersburg.
        (7987396,
         ForkID(hash=to_bytes(hexstr='0xafec6b27'), next=0),
         LocalChainIncompatibleOrStale),

        # Local is mainnet Muir Glacier, far in the future. Remote announces Gopherium (non
        # existing fork) at some future block 88888888, for itself, but past block for local.
        # Local is incompatible.
        #
        # This case detects non-upgraded nodes with majority hash power (typical Ropsten mess).
        (88888888,
         ForkID(hash=to_bytes(hexstr='0xe029e991'), next=88888888),
         LocalChainIncompatibleOrStale),

        # Local is mainnet Byzantium. Remote is also in Byzantium, but announces Gopherium (non
        # existing fork) at block 7279999, before Petersburg. Local is incompatible.
        (7279999,
         ForkID(hash=to_bytes(hexstr='0xa00bc324'), next=7279999),
         LocalChainIncompatibleOrStale),
    ]
)
def test_forkid_validation(local_head, remote_forkid, expected_error):
    if expected_error:
        with pytest.raises(expected_error):
            validate_forkid(
                remote_forkid, MAINNET_GENESIS_HASH, local_head, MAINNET_VM_CONFIGURATION)
    else:
        validate_forkid(remote_forkid, MAINNET_GENESIS_HASH, local_head, MAINNET_VM_CONFIGURATION)


@pytest.mark.parametrize(
    'forkid,expected_rlp',
    [
        (ForkID(hash=to_bytes(hexstr='0x00000000'), next=0), to_bytes(hexstr='0xc6840000000080')),
        (ForkID(hash=to_bytes(hexstr='0xdeadbeef'), next=int(0xBADDCAFE)),
         to_bytes(hexstr='0xca84deadbeef84baddcafe')),
    ]
)
def test_rlp_encoding(forkid, expected_rlp):
    assert rlp.encode(forkid) == expected_rlp
    assert rlp.decode(expected_rlp, sedes=ForkID) == forkid
