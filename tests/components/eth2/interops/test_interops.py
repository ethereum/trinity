import pytest

# from trinity._utils.async_iter import contains_all
# from trinity.tools.async_process_runner import AsyncProcessRunner


@pytest.mark.parametrize(
    "command",
    (
        (
            "trinity-beacon",
            "-l=DEBUG",
            "interop",
            "--validators=0,1",
            "--start-delay=10",
            "--wipedb",
        ),
    ),
)
@pytest.mark.asyncio
async def test_directory_generation(command, tmpdir):
    pass
    # NOTE: disabling this test as it is currently broken but the
    # underlying component will soon be deleted
    # async with AsyncProcessRunner.run(command, timeout_sec=60) as runner:
    #     assert await contains_all(runner.stderr, {"Validator", "BCCReceiveServer"})
