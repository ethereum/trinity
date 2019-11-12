import pkg_resources
from typing import (
    Tuple,
    Type,
)

from trinity.extensibility import (
    BaseComponentAPI,
)
from trinity.components.builtin.attach.component import (
    DbShellComponent,
    AttachComponent,
)
from trinity.components.builtin.beam_exec.component import (
    BeamChainExecutionComponent,
)
from trinity.components.builtin.beam_preview.component import (
    BeamChainPreviewComponent,
)
from trinity.components.builtin.ethstats.component import (
    EthstatsComponent,
)
from trinity.components.builtin.fix_unclean_shutdown.component import (
    FixUncleanShutdownComponent
)
from trinity.components.builtin.json_rpc.component import (
    JsonRpcServerComponent,
)
from trinity.components.builtin.network_db.component import (
    NetworkDBComponent,
)
from trinity.components.builtin.peer_discovery.component import (
    PeerDiscoveryComponent,
)
from trinity.components.builtin.request_server.component import (
    RequestServerComponent,
)
from trinity.components.builtin.syncer.component import (
    SyncerComponent,
)
from trinity.components.builtin.upnp.component import (
    UpnpComponent,
)
from trinity.components.eth2.beacon.component import BeaconNodeComponent
from trinity.components.eth2.interop.component import InteropComponent
from trinity.components.builtin.tx_pool.component import (
    TxComponent,
)


BASE_COMPONENTS: Tuple[Type[BaseComponentAPI], ...] = (
    AttachComponent,
    DbShellComponent,
    FixUncleanShutdownComponent,
    JsonRpcServerComponent,
    NetworkDBComponent,
    PeerDiscoveryComponent,
    UpnpComponent,
)

BEACON_NODE_COMPONENTS: Tuple[Type[BaseComponentAPI], ...] = (
    BeaconNodeComponent,
    InteropComponent,
)


ETH1_NODE_COMPONENTS: Tuple[Type[BaseComponentAPI], ...] = (
    BeamChainExecutionComponent,
    BeamChainPreviewComponent,
    EthstatsComponent,
    RequestServerComponent,
    SyncerComponent,
    TxComponent,
)


def discover_components() -> Tuple[Type[BaseComponentAPI], ...]:
    # Components need to define entrypoints at 'trinity.components' to automatically get loaded
    # https://packaging.python.org/guides/creating-and-discovering-components/#using-package-metadata

    return tuple(
        entry_point.load() for entry_point in pkg_resources.iter_entry_points('trinity.components')
    )


def get_all_components(*extra_components: Type[BaseComponentAPI],
                       ) -> Tuple[Type[BaseComponentAPI], ...]:
    return BASE_COMPONENTS + extra_components + discover_components()


def get_components_for_eth1_client() -> Tuple[Type[BaseComponentAPI], ...]:
    return get_all_components(*ETH1_NODE_COMPONENTS)


def get_components_for_beacon_client() -> Tuple[Type[BaseComponentAPI], ...]:
    return get_all_components(*BEACON_NODE_COMPONENTS)
