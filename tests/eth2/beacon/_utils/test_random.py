import pytest

from eth2.beacon._utils.random import (
    get_permuted_index,
    shuffle,
)


def slow_shuffle(items, seed):
    length = len(items)
    return tuple([items[get_permuted_index(i, length, seed)] for i in range(length)])


@pytest.mark.parametrize(
    (
        'values,seed'
    ),
    [
        (
            tuple(range(12)),
            b'\x23' * 32,
        ),
        (
            tuple(range(2**6))[10:],
            b'\x67' * 32,
        ),
    ],
)
def test_shuffle_consistent(values, seed):
    expect = slow_shuffle(values, seed)
    assert shuffle(values, seed) == expect
