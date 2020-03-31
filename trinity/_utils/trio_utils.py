import signal

import trio


async def wait_for_interrupts():
    with trio.open_signal_receiver(signal.SIGINT, signal.SIGTERM) as stream:
        async for _ in stream:
            return
