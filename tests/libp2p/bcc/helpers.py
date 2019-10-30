import asyncio


async def wait_until_true(predicate, timeout=1.0, retry=5):
    for _ in range(retry):
        if predicate():
            return True
        else:
            await asyncio.sleep(timeout / retry)
    raise asyncio.TimeoutError(f"Predicate has been False for {timeout} seconds")
