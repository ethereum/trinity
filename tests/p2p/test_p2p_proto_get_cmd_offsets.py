import pytest

from p2p.protocol import Protocol
from p2p.p2p_proto import BaseP2PProtocol


class With2(Protocol):
    cmd_length = 2


class With5(Protocol):
    cmd_length = 5


class With7(Protocol):
    cmd_length = 7


BASE_OFFSET = BaseP2PProtocol.cmd_length


@pytest.mark.parametrize(
    'protocols,offsets',
    (
        ((), ()),
        ((With2,), (BASE_OFFSET,)),
        ((With5,), (BASE_OFFSET,)),
        ((With7,), (BASE_OFFSET,)),
        ((With2, With5), (BASE_OFFSET, BASE_OFFSET + 2)),
        ((With5, With2), (BASE_OFFSET, BASE_OFFSET + 5)),
        ((With7, With2, With5), (BASE_OFFSET, BASE_OFFSET + 7, BASE_OFFSET + 7 + 2)),
    ),
)
def test_P2PProtocol_get_cmd_offsets(protocols, offsets):
    actual = BaseP2PProtocol.get_cmd_offsets(protocols)
    assert actual == offsets
