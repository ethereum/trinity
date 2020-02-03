import math

import pytest

from p2p.discv5.topic_table import TopicTable

from p2p.tools.factories.discovery import (
    ENRFactory,
    TopicFactory,
)


@pytest.fixture
def max_queue_size():
    return 5


@pytest.fixture
def max_total_size():
    return 15


@pytest.fixture
def target_ad_lifetime():
    return 5


@pytest.fixture
def topic_table(max_queue_size, max_total_size, target_ad_lifetime):
    return TopicTable(
        max_queue_size=max_queue_size,
        max_total_size=max_total_size,
        target_ad_lifetime=target_ad_lifetime,
    )


def test_table_size(topic_table, max_queue_size, target_ad_lifetime):
    assert len(topic_table) == 0

    topic = TopicFactory()
    topic_table.register(topic, ENRFactory(), 0)
    assert len(topic_table) == 1

    topic_table.register(topic, ENRFactory(), 0)
    assert len(topic_table) == 2

    topic2 = TopicFactory()
    topic_table.register(topic2, ENRFactory(), 0)
    assert len(topic_table) == 3

    topic3 = TopicFactory()
    for _ in range(max_queue_size):
        topic_table.register(topic3, ENRFactory(), 0)
    assert len(topic_table) == 3 + max_queue_size
    topic_table.register(topic3, ENRFactory(), target_ad_lifetime)
    assert len(topic_table) == 3 + max_queue_size


def test_wait_time(topic_table, max_queue_size, max_total_size, target_ad_lifetime):
    topic = TopicFactory()
    different_topic = TopicFactory()

    assert topic_table.get_wait_time(topic) == -math.inf

    start_time = 10
    time_delta = 2
    reg_times = tuple(start_time + index * time_delta for index in range(20))
    for index, reg_time in enumerate(reg_times):
        topic_table.register(topic, ENRFactory(), reg_time)

        oldest_topic_reg_time = reg_times[max(0, index + 1 - max_queue_size)]
        oldest_topic_eol = oldest_topic_reg_time + target_ad_lifetime

        if not topic_table.is_queue_full(topic):
            assert topic_table.get_wait_time(topic) == -math.inf
        else:
            assert topic_table.get_wait_time(topic) == oldest_topic_eol

        if not topic_table.is_table_full():
            assert topic_table.get_wait_time(different_topic) == -math.inf
        else:
            assert topic_table.get_wait_time(topic) == oldest_topic_eol


def test_registration(topic_table, max_queue_size, max_total_size, target_ad_lifetime):
    topic = TopicFactory()
    enr = ENRFactory()
    other_enr = ENRFactory()

    topic_table.get_enrs_for_topic(topic) == ()
    topic_table.register(topic, enr, 0)
    assert topic_table.get_enrs_for_topic(topic) == (enr,)
    topic_table.register(topic, other_enr, 0)
    assert topic_table.get_enrs_for_topic(topic) == (other_enr, enr)

    with pytest.raises(ValueError):
        topic_table.register(topic, enr, 0)

    while not topic_table.is_queue_full(topic):
        topic_table.register(topic, ENRFactory(), 0)
    with pytest.raises(ValueError):
        topic_table.register(topic, ENRFactory(), 0)
    topic_table.register(topic, enr, target_ad_lifetime)
    assert len(topic_table.get_enrs_for_topic(topic)) == max_queue_size
    assert topic_table.get_enrs_for_topic(topic)[0] == enr

    while not topic_table.is_full():
        topic_table.register(TopicFactory(), ENRFactory(), 0)
    with pytest.raises(ValueError):
        topic_table.register(TopicFactory(), ENRFactory(), 0)
