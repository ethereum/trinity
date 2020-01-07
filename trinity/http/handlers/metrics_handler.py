import logging

from aiohttp import web
from eth_utils.toolz import curry
from eth_typing import Hash32
from lahja.base import EndpointAPI
from prometheus_client import generate_latest

from eth2.beacon.chains.base import (
    BaseBeaconChain,
)


from trinity.http.handlers.base import BaseHTTPHandler, response_error
from trinity.components.eth2.metrics.events import (
    Libp2pPeersRequest,
)
from trinity.components.eth2.metrics.registry import metrics, registry


def root_to_int(root: Hash32) -> int:
    """
    Since prometheus couldn't collect bytes string, we convert Hash32 to int.
    """
    return int.from_bytes(root[24:32], byteorder='little', signed=True)


async def process_metrics(chain: BaseBeaconChain, event_bus: EndpointAPI) -> None:
    # Networking
    libp2p_peers = await event_bus.request(Libp2pPeersRequest())
    metrics.libp2p_peers.set(len(libp2p_peers.result))

    # Per slot info
    beacon_slot = chain.get_head_state_slot()
    metrics.beacon_slot.set(beacon_slot)

    # Per block info
    head_block = chain.get_canonical_head()
    metrics.beacon_head_slot.set(head_block.slot)
    metrics.beacon_head_root.set(root_to_int(head_block.hash_tree_root))

    # Per epoch info
    epoch_info = chain.get_canonical_epoch_info()
    metrics.beacon_previous_justified_epoch.set(epoch_info.previous_justified_checkpoint.epoch)
    metrics.beacon_previous_justified_root.set(root_to_int(
        epoch_info.previous_justified_checkpoint.root),
    )
    metrics.beacon_current_justified_epoch.set(epoch_info.current_justified_checkpoint.epoch)
    metrics.beacon_current_justified_root.set(
        root_to_int(epoch_info.current_justified_checkpoint.root),
    )
    metrics.beacon_finalized_epoch.set(epoch_info.finalized_checkpoint.epoch)
    metrics.beacon_finalized_root.set(root_to_int(epoch_info.finalized_checkpoint.root))


class MetricsHandler(BaseHTTPHandler):

    @staticmethod
    @curry
    async def handle(
        chain: BaseBeaconChain,
        event_bus: EndpointAPI,
        request: web.Request
    ) -> web.Response:
        logger = logging.getLogger('trinity.http.handlers.metrics_handler')
        try:
            if request.method == 'GET':
                logger.debug('Receiving GET request: %s', request.path)
                if request.path == '/metrics':
                    await process_metrics(chain, event_bus)
                    data = generate_latest(registry)
                    return web.Response(
                        body=data,
                        content_type='text/plain',
                    )

                return web.json_response({
                    'status': 'ok'
                })
            else:
                return response_error(f"Metrics Server doesn't support {request.method} request")
        except Exception as e:
            msg = f"[metrics_handler] Error: {e}"
            logger.error(msg)
            return response_error(msg)
