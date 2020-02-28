import eth_utils

from trinity._utils.logging import get_logger


def test_get_logger():
    logger = get_logger("foo.bar.baz")
    assert isinstance(logger, eth_utils.ExtendedDebugLogger)
    assert logger.parent.name == "foo.bar"
    assert isinstance(logger.parent, eth_utils.ExtendedDebugLogger)
    assert logger.parent.parent.name == "foo"
    assert isinstance(logger.parent.parent, eth_utils.ExtendedDebugLogger)


def test_get_logger_with_existing_ancestors():
    foo_logger = get_logger("foo")
    bar_logger = get_logger("foo.bar")
    assert bar_logger.parent == foo_logger
    baz_logger = get_logger("foo.bar.baz")
    assert baz_logger.parent == bar_logger
