import json
import logging
from asyncio.unix_events import _UnixSelectorEventLoop
from typing import Optional

from graphql.execution.executors.asyncio import AsyncioExecutor

from eth.chains.base import Chain

from .types import schema


def extract_errors(errors):
    if errors is None:
        return None
    return[error.message for error in errors]


class GraphQlServer:
    logger = logging.getLogger("GraphQlServer")

    def __init__(self, chain: Chain, loop: Optional[_UnixSelectorEventLoop]=None):
        self.chain = chain
        self.executor = AsyncioExecutor(loop=loop)

    async def execute(self, query: dict) -> str:
        # self.logger.info(f'got query {query["query"]}')
        result = await schema.execute(
            query['query'],
            executor=self.executor,
            context={'chain': self.chain},
            return_promise=True
        )
        self.logger.info(f'generated result {result.data}')
        self.logger.info(f'generated error {result.errors}')
        return json.dumps({
            'result': result.data,
            'errors': extract_errors(result.errors),
        })
