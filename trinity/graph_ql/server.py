import asyncio
import json
import logging

from graphql.execution.executors.asyncio import AsyncioExecutor

from eth.chains.base import Chain

from .types import schema


class GraphQlServer:
    logger = logging.getLogger("GraphQlServer")

    def __init__(self, chain: Chain):
        self.chain = chain
        self.executor = AsyncioExecutor(loop=asyncio.new_event_loop())

    async def execute(self, query: dict) -> str:
        self.logger.info(f'got query {query["query"]}')
        result = await schema.execute_async(query['query'], executor=AsyncioExecutor(), context={'chain': self.chain})
        self.logger.info(f'generated result {result.data}')
        self.logger.info(f'generated error {result.errors}')
        return json.dumps({
            'result': result.data,
            'errors': result.errors,
        })
