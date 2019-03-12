import asyncio
import functools

import pytest


@pytest.mark.asyncio
async def test_daemon_pubsub_get_topics(daemon_pubsubs, monkeypatch):
    ps = daemon_pubsubs[0]

    topics = []

    async def get_topics():
        return topics

    monkeypatch.setattr(ps.pubsub_client, "get_topics", get_topics)
    assert len(await ps.get_topics()) == 0
    topics.append("topic_123")
    assert len(await ps.get_topics()) == 1 and (await ps.get_topics())[0] == topics[0]

    # avoid error when cleaning up `daemon_pubsubs`
    while len(topics) != 0:
        topics.pop()


@pytest.mark.asyncio
async def test_daemon_pubsub_register_topic_validator(daemon_pubsubs):
    ps = daemon_pubsubs[0]

    async def validator(peer_id, ps_msg):
        return True

    assert len(ps._validators) == 0
    topic_0 = 'topic'
    topic_1 = 'topic_123'
    ps.register_topic_validator(topic_0, validator)
    assert len(ps._validators) == 1 and topic_0 in ps._validators
    ps.register_topic_validator(topic_1, validator)
    assert len(ps._validators) == 2 and topic_0 in ps._validators and topic_1 in ps._validators


@pytest.mark.asyncio
async def test_daemon_pubsub_subscribe(daemon_pubsubs):
    topic = "topic_123"
    assert len(await daemon_pubsubs[0].get_topics()) == 0
    assert len(daemon_pubsubs[0]._map_topic_stream) == 0
    assert len(daemon_pubsubs[0]._map_topic_task_listener) == 0
    assert len(daemon_pubsubs[0]._validators) == 0
    # test case: `subscribe`
    await daemon_pubsubs[0].subscribe(topic)
    assert (await daemon_pubsubs[0].get_topics()) == (topic,)
    assert topic in daemon_pubsubs[0]._map_topic_stream
    assert topic in daemon_pubsubs[0]._map_topic_task_listener
    assert topic not in daemon_pubsubs[0]._validators
    # test case: `subscribe` the same topic twice
    with pytest.raises(ValueError):
        await daemon_pubsubs[0].subscribe(topic)
    # test case: `subscribe` multiple topics
    another_topic = "topic_456"
    await daemon_pubsubs[0].subscribe(another_topic)
    assert len(await daemon_pubsubs[0].get_topics()) == 2
    assert len(daemon_pubsubs[0]._map_topic_stream) == 2
    assert len(daemon_pubsubs[0]._map_topic_task_listener) == 2
    assert len(daemon_pubsubs[0]._validators) == 0


@pytest.mark.asyncio
async def test_daemon_pubsub_unsubscribe(daemon_pubsubs):
    topic = "topic_123"
    await daemon_pubsubs[0].subscribe(topic)
    # test case: `unsubscribe`
    await daemon_pubsubs[0].unsubscribe(topic)
    assert len(await daemon_pubsubs[0].get_topics()) == 0
    assert len(daemon_pubsubs[0]._map_topic_stream) == 0
    assert len(daemon_pubsubs[0]._map_topic_task_listener) == 0
    assert len(daemon_pubsubs[0]._validators) == 0
    # test case: `unsubscribe` twice
    with pytest.raises(ValueError):
        await daemon_pubsubs[0].unsubscribe(topic)


@pytest.mark.parametrize(
    'num_hosts',
    (4,),
)
@pytest.mark.asyncio
async def test_daemon_pubsub_list_peers(daemon_hosts, daemon_pubsubs):
    # 0 <-> 1 <-> 2
    await daemon_hosts[0].connect(await daemon_hosts[1].get_peer_info())
    await daemon_hosts[1].connect(await daemon_hosts[2].get_peer_info())

    topic = "topic_123"
    # test case: returns empty when topic is non-subscribed
    assert len(await daemon_pubsubs[0].list_peers(topic)) == 0
    # test case: `list_peers` after subscriptions
    await daemon_pubsubs[0].subscribe(topic)
    assert len(await daemon_pubsubs[0].list_peers(topic)) == 0
    assert len(await daemon_pubsubs[1].list_peers(topic)) == 1
    assert len(await daemon_pubsubs[2].list_peers(topic)) == 0
    await daemon_pubsubs[2].subscribe(topic)
    assert len(await daemon_pubsubs[0].list_peers(topic)) == 0
    assert len(await daemon_pubsubs[1].list_peers(topic)) == 2
    assert len(await daemon_pubsubs[2].list_peers(topic)) == 0
    await daemon_pubsubs[1].subscribe(topic)
    assert len(await daemon_pubsubs[0].list_peers(topic)) == 1
    assert len(await daemon_pubsubs[1].list_peers(topic)) == 2
    assert len(await daemon_pubsubs[2].list_peers(topic)) == 1
    # test case: multiple topics don't affect each others
    another_topic = "topic_456"
    await daemon_pubsubs[1].subscribe(another_topic)
    assert len(await daemon_pubsubs[0].list_peers(topic)) == 1
    assert len(await daemon_pubsubs[1].list_peers(topic)) == 2
    assert len(await daemon_pubsubs[2].list_peers(topic)) == 1
    assert len(await daemon_pubsubs[0].list_peers(another_topic)) == 1
    assert len(await daemon_pubsubs[1].list_peers(another_topic)) == 0
    assert len(await daemon_pubsubs[2].list_peers(another_topic)) == 1
    # test case: `list_peers` still work even subscriptions are before connections
    await daemon_hosts[3].connect(await daemon_hosts[1].get_peer_info())
    assert len(await daemon_pubsubs[1].list_peers(topic)) == 2
    assert len(await daemon_pubsubs[1].list_peers(another_topic)) == 0


@pytest.mark.asyncio
async def test_daemon_pubsub_publish(daemon_hosts, daemon_pubsubs):
    # 0 <-> 1 <-> 2
    await daemon_hosts[0].connect(await daemon_hosts[1].get_peer_info())
    await daemon_hosts[1].connect(await daemon_hosts[2].get_peer_info())

    topic = "topic_123"

    # test case: `publish` and validator
    await daemon_pubsubs[0].subscribe(topic)
    await daemon_pubsubs[1].subscribe(topic)
    await daemon_pubsubs[2].subscribe(topic)

    data = b'123'

    async def validator(src_peer_id, ps_msg, event):
        assert ps_msg.data == data
        event.set()

    event_0 = asyncio.Event()
    event_1 = asyncio.Event()
    event_2 = asyncio.Event()
    daemon_pubsubs[0].register_topic_validator(topic, functools.partial(validator, event=event_0))
    daemon_pubsubs[1].register_topic_validator(topic, functools.partial(validator, event=event_1))
    daemon_pubsubs[2].register_topic_validator(topic, functools.partial(validator, event=event_2))

    await daemon_pubsubs[0].publish(topic, data)

    await event_0.wait()
    await event_1.wait()
    await event_2.wait()
