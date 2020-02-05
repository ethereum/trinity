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


def test_wait_time_full_table(topic_table, target_ad_lifetime):
    # fill one queue
    reg_time = 0
    oldest_table_eol = reg_time + target_ad_lifetime
    while not topic_table.is_full():
        assert topic_table.get_wait_time(TopicFactory(), 0) == 0
        topic_table.register(TopicFactory(), ENRFactory(), reg_time)
        reg_time += 1

    assert topic_table.get_wait_time(TopicFactory(), 0) == oldest_table_eol
    topic_table.register(TopicFactory(), ENRFactory(), reg_time)
    assert topic_table.get_wait_time(TopicFactory(), 0) == oldest_table_eol + 1


def test_wait_time_full_queue(topic_table, max_total_size, target_ad_lifetime):
    topic = TopicFactory()
    different_topic = TopicFactory()

    reg_time = 0
    oldest_queue_eol = reg_time + target_ad_lifetime
    while not topic_table.is_queue_full(topic):
        assert topic_table.get_wait_time(topic, 0) == 0
        assert topic_table.get_wait_time(different_topic, 0) == 0
        topic_table.register(topic, ENRFactory(), reg_time)
        reg_time += 1

    assert topic_table.get_wait_time(topic, 0) == oldest_queue_eol
    assert topic_table.get_wait_time(different_topic, 0) == 0
    topic_table.register(topic, ENRFactory(), reg_time)
    assert topic_table.get_wait_time(topic, 0) == oldest_queue_eol + 1
    assert topic_table.get_wait_time(different_topic, 0) == 0


def test_wait_time_full_queue_and_table(topic_table, max_queue_size, target_ad_lifetime):
    # fill one queue
    topic = TopicFactory()
    reg_time = 0
    oldest_queue_eol = reg_time + target_ad_lifetime
    while not topic_table.is_queue_full(topic):
        topic_table.register(topic, ENRFactory(), reg_time)
        reg_time += 1

    # fill the rest of the table
    oldest_table_eol = reg_time + target_ad_lifetime
    while not topic_table.is_full():
        topic_table.register(TopicFactory(), ENRFactory(), reg_time)
        reg_time += 1

    assert topic_table.get_wait_time(topic, 0) == oldest_queue_eol
    assert topic_table.get_wait_time(TopicFactory(), 0) == oldest_queue_eol

    # refill queue
    oldest_queue_eol = reg_time + target_ad_lifetime
    for _ in range(max_queue_size):
        topic_table.register(topic, ENRFactory(), reg_time)
        reg_time += 1

    assert topic_table.get_wait_time(topic, 0) == oldest_queue_eol
    assert topic_table.get_wait_time(TopicFactory(), 0) == oldest_table_eol


def test_registration_single_queue(topic_table, max_queue_size):
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

    enrs_before = topic_table.get_enrs_for_topic(topic)
    new_enr = ENRFactory()
    topic_table.register(topic, new_enr, topic_table.get_wait_time(topic, 0))
    enrs_after = topic_table.get_enrs_for_topic(topic)
    assert enrs_after == (new_enr,) + enrs_before[:-1]


def test_registration_two_queues(topic_table, max_queue_size):
    topic1 = TopicFactory()
    topic2 = TopicFactory()
    enr = ENRFactory()

    topic_table.register(topic1, enr, 0)
    while not topic_table.is_queue_full(topic1):
        topic_table.register(topic1, ENRFactory(), 0)

    topic_table.register(topic2, enr, 1)
    while not topic_table.is_queue_full(topic2):
        topic_table.register(topic2, ENRFactory(), 1)

    with pytest.raises(ValueError):
        topic_table.register(topic1, ENRFactory(), 1)
    with pytest.raises(ValueError):
        topic_table.register(topic2, ENRFactory(), 1)
    with pytest.raises(ValueError):
        topic_table.register(topic2, ENRFactory(), topic_table.get_wait_time(topic1, 0))

    enrs_topic1_before = topic_table.get_enrs_for_topic(topic1)
    enrs_topic2_before = topic_table.get_enrs_for_topic(topic2)
    new_enr_topic1 = ENRFactory()
    new_enr_topic2 = ENRFactory()

    topic_table.register(topic1, new_enr_topic1, topic_table.get_wait_time(topic1, 0))
    topic_table.register(topic2, new_enr_topic2, topic_table.get_wait_time(topic2, 0))

    enrs_topic1_after = topic_table.get_enrs_for_topic(topic1)
    enrs_topic2_after = topic_table.get_enrs_for_topic(topic2)
    assert enrs_topic1_after == (new_enr_topic1,) + enrs_topic1_before[:-1]
    assert enrs_topic2_after == (new_enr_topic2,) + enrs_topic2_before[:-1]


def test_registration_full_table(topic_table, max_queue_size, max_total_size):
    for _ in range(max_total_size):
        topic_table.register(TopicFactory(), ENRFactory(), 0)
    assert topic_table.is_full()

    with pytest.raises(ValueError):
        topic_table.register(TopicFactory(), ENRFactory(), 0)
    wait_time = topic_table.get_wait_time(TopicFactory(), 0)
    topic_table.register(TopicFactory(), ENRFactory(), wait_time)

    topic = TopicFactory()
    assert not topic_table.is_queue_full(topic)
    while not topic_table.is_queue_full(topic):
        topic_table.register(topic, ENRFactory(), topic_table.get_wait_time(topic, 0))

    with pytest.raises(ValueError):
        topic_table.register(TopicFactory(), ENRFactory(), 0)
    wait_time = topic_table.get_wait_time(TopicFactory(), 0)
    topic_table.register(TopicFactory(), ENRFactory(), wait_time)
