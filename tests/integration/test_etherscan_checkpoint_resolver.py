import pytest

from eth_utils import encode_hex
from trinity.components.builtin.syncer.cli import (
    parse_checkpoint_uri,
    is_block_hash,
)
from trinity.constants import (
    MAINNET_NETWORK_ID,
    GOERLI_NETWORK_ID,
    ROPSTEN_NETWORK_ID,
)


# These are just arbitrarily choosen scores that we know can serve as a rough validity check.
MIN_EXPECTED_MAINNET_SCORE = 11631608640717612820968
MIN_EXPECTED_GOERLI_SCORE = 4216548
MIN_EXPECTED_ROPSTEN_SCORE = 30423839501145616


@pytest.mark.parametrize(
    'uri,network_id,min_expected_score',
    (
        ('eth://block/byetherscan/latest', MAINNET_NETWORK_ID, MIN_EXPECTED_MAINNET_SCORE),
        ('eth://block/byetherscan/latest', GOERLI_NETWORK_ID, MIN_EXPECTED_GOERLI_SCORE),
        ('eth://block/byetherscan/latest', ROPSTEN_NETWORK_ID, MIN_EXPECTED_ROPSTEN_SCORE)
    )
)
def test_parse_checkpoint(uri, network_id, min_expected_score):
    checkpoint = parse_checkpoint_uri(uri, network_id)
    assert checkpoint.score >= min_expected_score
    assert is_block_hash(encode_hex(checkpoint.block_hash))
