from eth.constants import ZERO_HASH32
from eth.exceptions import BlockNotFound
from eth.validation import validate_word
import pytest

from eth2.beacon.constants import EMPTY_SIGNATURE
from eth2.beacon.types.blocks import BeaconBlock, BeaconBlockBody
from trinity.protocol.bcc_libp2p import node
from trinity.protocol.bcc_libp2p.configs import GoodbyeReasonCode, ResponseCode
from trinity.protocol.bcc_libp2p.exceptions import HandshakeFailure, RequestFailure
from trinity.protocol.bcc_libp2p.messages import Status
from trinity.protocol.bcc_libp2p.node import REQ_RESP_STATUS_SSZ
from trinity.protocol.bcc_libp2p.utils import read_req, write_resp
from trinity.tools.async_method import wait_until_true
from trinity.tools.bcc_factories import ConnectionPairFactory


@pytest.mark.asyncio
async def test_handshake_success():
    async with ConnectionPairFactory(handshake=False) as (alice, bob):
        await alice.request_status(bob.peer_id)
        assert bob.peer_id in alice.handshaked_peers
        assert alice.peer_id in bob.handshaked_peers


@pytest.mark.asyncio
async def test_handshake_failure_invalid_status_packet(monkeypatch, mock_timeout):
    async with ConnectionPairFactory(handshake=False) as (alice, bob):

        def status_with_wrong_fork_version(chain):
            return Status.create(
                head_fork_version=b"\x12\x34\x56\x78"  # version different from another node.
            )

        monkeypatch.setattr(node, "get_my_status", status_with_wrong_fork_version)
        with pytest.raises(HandshakeFailure):
            await alice.request_status(bob.peer_id)
        assert alice.peer_id not in bob.handshaked_peers
        assert bob.peer_id not in alice.handshaked_peers

        def status_with_wrong_checkpoint(chain):
            return Status.create(
                finalized_root=b"\x78"
                * 32  # finalized root different from another node.
            )

        monkeypatch.setattr(node, "get_my_status", status_with_wrong_checkpoint)
        with pytest.raises(HandshakeFailure):
            await alice.request_status(bob.peer_id)
        assert alice.peer_id not in bob.handshaked_peers
        assert bob.peer_id not in alice.handshaked_peers


@pytest.mark.asyncio
async def test_handshake_failure_response():
    async with ConnectionPairFactory(handshake=False) as (alice, bob):

        async def fake_handle_status(stream):
            await read_req(stream, Status)
            # The overridden `resp_code` can be anything other than `ResponseCode.SUCCESS`
            await write_resp(stream, "error msg", ResponseCode.INVALID_REQUEST)

        # Mock the handler.
        bob.host.set_stream_handler(REQ_RESP_STATUS_SSZ, fake_handle_status)
        # Test: Handshake fails when the response is not success.
        with pytest.raises(HandshakeFailure):
            await alice.request_status(bob.peer_id)
        assert alice.peer_id not in bob.handshaked_peers


@pytest.mark.asyncio
async def test_goodbye():
    async with ConnectionPairFactory() as (alice, bob):
        await alice.say_goodbye(bob.peer_id, GoodbyeReasonCode.FAULT_OR_ERROR)
        assert bob.peer_id not in alice.handshaked_peers
        assert await wait_until_true(lambda: alice.peer_id not in bob.handshaked_peers)


@pytest.mark.asyncio
async def test_request_beacon_blocks_before_handshake():
    async with ConnectionPairFactory(handshake=False) as (alice, bob):
        # Test: Can not request beacon block before handshake
        with pytest.raises(RequestFailure):
            await alice.request_beacon_blocks_by_range(
                peer_id=bob.peer_id,
                head_block_root=ZERO_HASH32,
                start_slot=0,
                count=1,
                step=1,
            )

        # Test: Can not request beacon block by root before handshake
        with pytest.raises(RequestFailure):
            await alice.request_beacon_blocks_by_root(
                peer_id=bob.peer_id, block_roots=[b"\x12" * 32]
            )


@pytest.mark.asyncio
async def test_request_beacon_blocks_by_range_invalid_request(monkeypatch):
    async with ConnectionPairFactory() as (alice, bob):

        head_slot = 1
        request_head_block_root = b"\x56" * 32
        head_block = BeaconBlock.create(
            slot=head_slot,
            parent_root=ZERO_HASH32,
            state_root=ZERO_HASH32,
            signature=EMPTY_SIGNATURE,
            body=BeaconBlockBody.create(),
        )

        # TEST: Can not request blocks with `start_slot` greater than head block slot
        start_slot = 2

        def get_block_by_root(root):
            return head_block

        monkeypatch.setattr(bob.chain, "get_block_by_root", get_block_by_root)

        count = 1
        step = 1
        with pytest.raises(RequestFailure):
            await alice.request_beacon_blocks_by_range(
                peer_id=bob.peer_id,
                head_block_root=request_head_block_root,
                start_slot=start_slot,
                count=count,
                step=step,
            )

        # TEST: Can not request fork chain blocks with `start_slot`
        # lower than peer's latest finalized slot
        start_slot = head_slot
        state_machine = bob.chain.get_state_machine()
        old_state = bob.chain.get_head_state()
        new_checkpoint = old_state.finalized_checkpoint.set(
            "epoch", old_state.finalized_checkpoint.epoch + 1
        )

        def get_canonical_block_by_slot(slot):
            raise BlockNotFound

        monkeypatch.setattr(
            bob.chain, "get_canonical_block_by_slot", get_canonical_block_by_slot
        )

        def get_state_machine(at_slot=None):
            class MockStateMachine:
                state = old_state.set("finalized_checkpoint", new_checkpoint)
                config = state_machine.config

            return MockStateMachine()

        def get_head_state():
            return old_state.set("finalized_checkpoint", new_checkpoint)

        monkeypatch.setattr(bob.chain, "get_state_machine", get_state_machine)
        monkeypatch.setattr(bob.chain, "get_head_state", get_head_state)

        with pytest.raises(RequestFailure):
            await alice.request_beacon_blocks_by_range(
                peer_id=bob.peer_id,
                head_block_root=request_head_block_root,
                start_slot=start_slot,
                count=count,
                step=step,
            )


@pytest.mark.asyncio
async def test_request_beacon_blocks_by_range_on_nonexist_chain(monkeypatch):
    async with ConnectionPairFactory() as (alice, bob):

        request_head_block_root = b"\x56" * 32

        def get_block_by_root(root):
            raise BlockNotFound

        monkeypatch.setattr(bob.chain, "get_block_by_root", get_block_by_root)

        start_slot = 0
        count = 5
        step = 1
        requested_blocks = await alice.request_beacon_blocks_by_range(
            peer_id=bob.peer_id,
            head_block_root=request_head_block_root,
            start_slot=start_slot,
            count=count,
            step=step,
        )

        assert len(requested_blocks) == 0


@pytest.mark.asyncio
async def test_request_beacon_blocks_by_root(monkeypatch):
    async with ConnectionPairFactory() as (alice, bob):

        # Mock up block database
        head_block = BeaconBlock.create(
            slot=0,
            parent_root=ZERO_HASH32,
            state_root=ZERO_HASH32,
            signature=EMPTY_SIGNATURE,
            body=BeaconBlockBody.create(),
        )
        blocks = [head_block.set("slot", slot) for slot in range(5)]
        mock_root_to_block_db = {block.signing_root: block for block in blocks}

        def get_block_by_root(root):
            validate_word(root)
            if root in mock_root_to_block_db:
                return mock_root_to_block_db[root]
            else:
                raise BlockNotFound

        monkeypatch.setattr(bob.chain, "get_block_by_root", get_block_by_root)

        requesting_block_roots = [
            blocks[0].signing_root,
            b"\x12" * 32,  # Unknown block root
            blocks[1].signing_root,
            b"\x23" * 32,  # Unknown block root
            blocks[3].signing_root,
        ]
        requested_blocks = await alice.request_beacon_blocks_by_root(
            peer_id=bob.peer_id, block_roots=requesting_block_roots
        )

        expected_blocks = [blocks[0], blocks[1], blocks[3]]
        assert len(requested_blocks) == len(expected_blocks)
        assert set(requested_blocks) == set(expected_blocks)
