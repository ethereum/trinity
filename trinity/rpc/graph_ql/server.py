import json
import logging
from asyncio.unix_events import _UnixSelectorEventLoop
from typing import (
    Any,
    AnyStr,
    List,
    Dict,
    Optional,
)

from graphql import GraphQLError
from graphql.execution.executors.asyncio import AsyncioExecutor

from trinity.rpc.abc import BaseRPCServer
from trinity.rpc.graph_ql.graphiql import graphiql_html
from trinity.rpc.json_rpc.modules import Eth1ChainRPCModule
from trinity.rpc.typing import Response
from .types import schema


def extract_errors(errors: GraphQLError) -> Optional[List[str]]:
    if errors is None:
        return None
    return[error.message for error in errors]


class GraphQlServer(BaseRPCServer):
    logger = logging.getLogger("GraphQlServer")
    supported_methods = ['GET', 'POST']

    def __init__(self, chainrpc: Eth1ChainRPCModule, loop: Optional[_UnixSelectorEventLoop]=None):
        self.chainrpc = chainrpc
        self.executor = AsyncioExecutor(loop=loop)

    async def execute_post(self, query: Dict[str, AnyStr]) -> str:
        result = await schema.execute(
            query['query'],
            executor=self.executor,
            context=dict(
                chain=self.chainrpc.chain,
                event_bus=self.chainrpc.event_bus
            ),
            return_promise=True
        )
        return json.dumps({
            'data': result.data,
            'errors': extract_errors(result.errors),
        })

    async def execute_get(self, request: Dict[str, Any]) -> Response:
        return Response(content_type='html', body=graphiql_html)
