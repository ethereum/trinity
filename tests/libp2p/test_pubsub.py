import asyncio
import functools

import pytest


@pytest.mark.parametrize(
    'num_hosts',
    (4,),
)
@pytest.mark.asyncio
async def test_daemon_pubsub(daemon_hosts, daemon_pubsubs):
    # 0 <-> 1 <-> 2
    await daemon_hosts[0].connect(await daemon_hosts[1].get_peer_info())
    await daemon_hosts[1].connect(await daemon_hosts[2].get_peer_info())

    topic = "topic123"

    # test case: `subscribe`
    await daemon_pubsubs[0].subscribe(topic)
    assert (await daemon_pubsubs[0].get_topics()) == (topic,)
    # test case: `subscribe` twice
    with pytest.raises(ValueError):
        await daemon_pubsubs[0].subscribe(topic)
    # test case: `unsubscribe`
    await daemon_pubsubs[0].unsubscribe(topic)
    assert len(await daemon_pubsubs[0].get_topics()) == 0
    # test case: `unsubscribe` twice
    with pytest.raises(ValueError):
        await daemon_pubsubs[0].unsubscribe(topic)

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
