import pytest

from trinity._utils.async_iter import async_take


async def empty_iterator():
    # trick to convince python that this is an iterator, despite yielding nothing
    if False:
        yield "NEVER"
        raise AssertionError("This should never have been yielded in the first place")


async def single_val_iterator():
    yield "just_once"


async def canned_iterator():
    while True:
        yield "spam"


@pytest.mark.parametrize(
    "take_count, iterator, expected_list",
    (
        (0, empty_iterator(), []),
        (1, empty_iterator(), []),
        (2, empty_iterator(), []),
        (0, single_val_iterator(), []),
        (1, single_val_iterator(), ["just_once"]),
        (2, single_val_iterator(), ["just_once"]),
        (0, canned_iterator(), []),
        (1, canned_iterator(), ["spam"]),
        (2, canned_iterator(), ["spam", "spam"]),
    ),
)
@pytest.mark.asyncio
async def test_async_take(take_count, iterator, expected_list):
    actual_result = [val async for val in async_take(take_count, iterator)]
    assert actual_result == expected_list
