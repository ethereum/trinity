import json
from typing import (
    Any,
    Dict,
    Sequence,
    Tuple,
    Union,
    Type,
)

from lahja import EndpointAPI

from eth_utils import (
    get_logger,
    ValidationError,
    ExtendedDebugLogger,
)
from eth_utils.toolz import curry

from trinity.chains.base import AsyncChainAPI
from trinity.exceptions import RpcError
from trinity.rpc.modules import (
    BaseRPCModule,
)
from trinity.rpc.retry import (
    execute_with_retries,
)

REQUIRED_REQUEST_KEYS = {
    'id',
    'jsonrpc',
    'method',
}


def validate_request(request: Dict[str, Any]) -> None:
    missing_keys = REQUIRED_REQUEST_KEYS - set(request.keys())
    if missing_keys:
        raise ValueError("request must include the keys: %r" % missing_keys)


def generate_response(request: Dict[str, Any], result: Any, error: Union[Exception, str]) -> str:
    response = {
        'id': request.get('id', -1),
        'jsonrpc': request.get('jsonrpc', "2.0"),
    }

    if error is None:
        response['result'] = result
    elif result is not None:
        raise ValueError("Must not supply both a result and an error for JSON-RPC response")
    else:
        # only error is not None
        response['error'] = str(error)

    return json.dumps(response)


class RPCServer:
    """
    This "server" accepts json strings requests and returns the appropriate json string response,
    meeting the protocol for JSON-RPC defined here: https://github.com/ethereum/wiki/wiki/JSON-RPC

    The key entry point for all requests is :meth:`RPCServer.execute`, which
    then proxies to the appropriate method. For example, see
    :meth:`RPCServer.eth_getBlockByHash`.
    """
    chain = None

    def __init__(self,
                 modules: Sequence[BaseRPCModule],
                 chain: AsyncChainAPI,
                 event_bus: EndpointAPI = None) -> None:
        self.event_bus = event_bus
        self.modules: Dict[str, BaseRPCModule] = {}
        self.chain = chain
        self.logger: ExtendedDebugLogger = get_logger('trinity.rpc.main.RPCServer')

        for module in modules:
            name = module.get_name()

            if name in self.modules:
                raise ValueError(
                    f"Apparent name conflict in registered RPC modules, {name} already registered"
                )

            self.modules[name] = module

    def _lookup_method(self,
                       rpc_method: str,
                       disallowed_modules: Sequence[Type[BaseRPCModule]]) -> Any:
        method_pieces = rpc_method.split('_')

        if len(method_pieces) != 2:
            # This check provides a security guarantee: that it's impossible to invoke
            # a method with an underscore in it. Only public methods on the modules
            # will be callable by external clients.
            raise ValueError("Invalid RPC method: %r" % rpc_method)
        module_name, method_name = method_pieces

        if module_name not in self.modules:
            raise ValueError("Module unavailable: %r" % module_name)
        module = self.modules[module_name]

        if type(module) in disallowed_modules:
            raise ValidationError(f"Access of {module.get_name()} module prohibited")

        try:
            return getattr(module, method_name)
        except AttributeError:
            raise ValueError("Method not implemented: %r" % rpc_method)

    async def _get_result(self,
                          request: Dict[str, Any],
                          disallowed_modules: Sequence[Type[BaseRPCModule]] = (),
                          debug: bool = False) -> Tuple[Any, Union[Exception, str]]:
        """
        :returns: (result, error) - result is None if error is provided. Error must be
            convertable to string with ``str(error)``.
        """
        try:
            validate_request(request)

            if request.get('jsonrpc', None) != '2.0':
                raise NotImplementedError("Only the 2.0 jsonrpc protocol is supported")

            method = self._lookup_method(request['method'], disallowed_modules)

            params = request.get('params', [])

            result = await execute_with_retries(
                self.event_bus, method, params, self.chain,
            )

            if request['method'] == 'evm_resetToGenesisFixture':
                result = True

        except TypeError as exc:
            error = f"Invalid parameters. Check parameter count and types. {exc}"
            if debug:
                raise
            return None, error
        except NotImplementedError as exc:
            error = "Method not implemented: %r %s" % (request['method'], exc)
            if debug:
                raise
            return None, error
        except ValidationError as exc:
            self.logger.debug("Validation error while executing RPC method", exc_info=True)
            if debug:
                raise
            return None, exc
        except RpcError as exc:
            self.logger.info(exc)
            if debug:
                raise
            return None, exc
        except Exception as exc:
            self.logger.warning("RPC method caused exception")
            if debug:
                raise
            return None, exc
        else:
            return result, None

    async def execute(self,
                      request: Dict[str, Any]) -> str:
        """
        Delegate to :meth:`~trinity.rpc.main.RPCServer.execute_with_access_control` with
        unrestricted access.
        """
        return await self.execute_with_access_control((), request)

    @curry
    async def execute_with_access_control(
            self,
            disallowed_modules: Sequence[Type[BaseRPCModule]],
            request: Dict[str, Any]) -> str:
        """
        The key entry point for all incoming requests. Execution of requests to certain modules
        can be restricted by providing a sequence of ``disallowed_modules`` to this API. An empty
        sequence of modules allows requests to be made for all modules.
        Access restriction happens on this level because one instance of the server may allow or
        prevent execution of certain requests based on external conditions (e.g request origin).
        """
        result, error = await self._get_result(request, disallowed_modules)
        return generate_response(request, result, error)
