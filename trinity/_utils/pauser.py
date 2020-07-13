import asyncio


class Pauser:
    """
    A helper class to provide pause / resume functionality to other classes.
    """

    def __init__(self) -> None:
        self._paused = False
        self._resumed = asyncio.Event()

    @property
    def is_paused(self) -> bool:
        """
        Return ``True`` if the state of managed operation is paused, otherwise ``False``.
        """
        return self._paused

    def pause(self) -> None:
        """
        Pause the managed operation.
        """
        if self._paused:
            raise RuntimeError(
                "Invalid action. Can not pause an operation that is already paused."
            )

        self._paused = True

    def resume(self) -> None:
        """
        Resume the operation.
        """
        if not self._paused:
            raise RuntimeError("Invalid action. Can not resume operation that isn't paused.")

        self._paused = False
        self._resumed.set()

    async def await_resume(self) -> None:
        """
        Await until ``resume()`` is called. Throw if called when the operation is not paused.
        """
        if not self._paused:
            raise RuntimeError("Can not await resume on operation that isn't paused.")

        await self._resumed.wait()
        self._resumed.clear()
