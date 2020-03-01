import functools
import pickle
import socket

import pytest

from rlp import sedes

from eth.chains.mainnet import MAINNET_GENESIS_HEADER, MAINNET_VM_CONFIGURATION
from eth.chains.ropsten import ROPSTEN_GENESIS_HEADER, ROPSTEN_VM_CONFIGURATION

from p2p.discv5.constants import IP_V4_ADDRESS_ENR_KEY, TCP_PORT_ENR_KEY, UDP_PORT_ENR_KEY
from p2p.discv5.typing import NodeID
from p2p.kademlia import Node
from p2p.tools.factories.discovery import ENRFactory
from p2p.tools.factories.kademlia import IPAddressFactory, NodeFactory

from trinity.exceptions import BaseForkIDValidationError, ENRMissingForkID
from trinity.protocol.common.peer import skip_candidate_if_on_list_or_fork_mismatch
from trinity.protocol.eth import forkid


def test_skip_candidate_if_on_list_or_fork_mismatch_is_pickleable():
    fork_blocks = forkid.extract_fork_blocks(MAINNET_VM_CONFIGURATION)
    skip_list = [NodeID(b'')]
    partial_fn = functools.partial(
        skip_candidate_if_on_list_or_fork_mismatch,
        MAINNET_GENESIS_HEADER.hash,
        MAINNET_GENESIS_HEADER.block_number,
        fork_blocks,
        skip_list,
    )
    unpickled = pickle.loads(pickle.dumps(partial_fn))
    assert unpickled.func == partial_fn.func
    assert unpickled.args == partial_fn.args


def _make_node_with_enr_and_forkid(genesis_hash, head, vm_config):
    fork_blocks = forkid.extract_fork_blocks(vm_config)
    node_forkid = forkid.make_forkid(genesis_hash, head, fork_blocks)
    ip = socket.inet_aton(IPAddressFactory.generate())
    udp_port = 30304
    enr = ENRFactory(
        custom_kv_pairs={
            b'eth': sedes.List([forkid.ForkID]).serialize([node_forkid]),
            IP_V4_ADDRESS_ENR_KEY: ip,
            UDP_PORT_ENR_KEY: udp_port,
            TCP_PORT_ENR_KEY: udp_port,
        }
    )
    return Node(enr)


def test_skip_candidate_if_on_list_or_fork_mismatch():
    mainnet_fork_blocks = forkid.extract_fork_blocks(MAINNET_VM_CONFIGURATION)
    should_skip_fn = functools.partial(
        skip_candidate_if_on_list_or_fork_mismatch,
        MAINNET_GENESIS_HEADER.hash,
        MAINNET_GENESIS_HEADER.block_number,
        mainnet_fork_blocks,
    )
    no_forkid_nodes = NodeFactory.create_batch(10)
    compatible_node = _make_node_with_enr_and_forkid(
        MAINNET_GENESIS_HEADER.hash, MAINNET_GENESIS_HEADER.block_number, MAINNET_VM_CONFIGURATION)
    # Ensure compatible_node's forkid is compatible with our current chain state.
    forkid.validate_forkid(
        forkid.extract_forkid(compatible_node.enr),
        MAINNET_GENESIS_HEADER.hash,
        MAINNET_GENESIS_HEADER.block_number,
        mainnet_fork_blocks,
    )

    # It returns True for candidates on the skip list, even if they are fork-id compatible.
    skip_list = [no_forkid_nodes[1].id, compatible_node.id]
    assert functools.partial(should_skip_fn, skip_list)(compatible_node) is True
    assert functools.partial(should_skip_fn, skip_list)(no_forkid_nodes[1]) is True

    # It returns False for candidates with no fork-id that are not on the skip list.
    with pytest.raises(ENRMissingForkID):
        assert forkid.extract_forkid(no_forkid_nodes[0].enr)
    assert functools.partial(should_skip_fn, skip_list)(no_forkid_nodes[0]) is False

    # It returns False for candidates with compatible fork-ids that are not on the skip list
    assert functools.partial(should_skip_fn, [])(compatible_node) is False

    # It returns True for candidates with incompatible fork-ids
    incompatible_node = _make_node_with_enr_and_forkid(
        ROPSTEN_GENESIS_HEADER.hash, ROPSTEN_GENESIS_HEADER.block_number, ROPSTEN_VM_CONFIGURATION)
    with pytest.raises(BaseForkIDValidationError):
        forkid.validate_forkid(
            forkid.extract_forkid(incompatible_node.enr),
            MAINNET_GENESIS_HEADER.hash,
            MAINNET_GENESIS_HEADER.block_number,
            mainnet_fork_blocks,
        )
    assert functools.partial(should_skip_fn, [])(incompatible_node) is True
