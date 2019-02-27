from typing import (
    AsyncIterator,
    Awaitable,
    TypeVar,
)

from cancel_token import CancelToken

TReturn = TypeVar('TReturn')


class CancellableMixin:
    cancel_token: CancelToken = None

    async def wait(self,
                   awaitable: Awaitable[TReturn],
                   token: CancelToken = None,
                   timeout: float = None) -> TReturn:
        """See wait_first()"""
        return await self.wait_first(awaitable, token=token, timeout=timeout)

    async def wait_first(self,
                         *awaitables: Awaitable[TReturn],
                         token: CancelToken = None,
                         timeout: float = None) -> TReturn:
        """
        Wait for the first awaitable to complete, unless we timeout or the token chain is triggered.

        The given token is chained with this service's token, so triggering either will cancel
        this.

        Returns the result of the first one to complete.

        Raises TimeoutError if we timeout or OperationCancelled if the token chain is triggered.

        All pending futures are cancelled before returning.
        """
        token_chain = self._get_token_chain(token)
        return await token_chain.cancellable_wait(*awaitables, timeout=timeout)

    async def wait_iter(
            self,
            aiterable: AsyncIterator[TReturn],
            token: CancelToken = None,
            timeout: float = None) -> AsyncIterator[TReturn]:
        """
        Iterate through an async iterator, raising the OperationCancelled exception if the token is
        triggered. For example:

        ::

            async for val in self.wait_iter(my_async_iterator()):
                do_stuff(val)

        See :meth:`CancellableMixin.wait_first` for using arguments ``token`` and ``timeout``
        """
        token_chain = self._get_token_chain(token)
        aiter = aiterable.__aiter__()

        # A CancelToken, only to propagate back the StopAsyncIteration
        aiter_token = CancelToken('__anext__')
        while not token_chain.triggered and not aiter_token.triggered:
            val = await self.wait(
                self._handle_anext(aiter, aiter_token),
                token=token,
                timeout=timeout,
            )

            if aiter_token.triggered:
                break
            else:
                yield val

    async def _handle_anext(self,
                            aiter: AsyncIterator[TReturn],
                            aiter_token: CancelToken) -> TReturn:
        """
        Call __anext__ on an AsyncIterator[TReturn] and handle StopAsyncIteration exceptions
        by triggering the passed aiter_token in order to propagate it back.
        """
        try:
            return await aiter.__anext__()
        except StopAsyncIteration:
            aiter_token.trigger()
            return None  # mypy wants this

    def _get_token_chain(self, token: CancelToken = None) -> CancelToken:
        return self.cancel_token if token is None else token.chain(self.cancel_token)
