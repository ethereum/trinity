from abc import (
    ABC,
    abstractmethod,
)
import argparse
from contextlib import (
    contextmanager,
)
from enum import (
    Enum,
    auto,
)
import hashlib
import json
from pathlib import (
    Path,
)
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

from eth.abc import (
    AtomicDatabaseAPI,
    ConsensusContextAPI,
)
from eth.consensus import (
    CliqueApplier,
    ConsensusApplier,
    ConsensusContext,
    CliqueConsensusContext,
    NoProofConsensus,
    PowConsensus,
)

from eth.typing import (
    VMConfiguration,
)
from eth_keys import (
    keys,
)
from eth_keys.datatypes import (
    PrivateKey,
)
from eth_utils import encode_hex
from eth_typing import (
    Address,
    BLSPubkey,
)

from multiaddr import (
    Multiaddr,
)

from eth2.beacon.chains.testnet import SkeletonLakeChain
from eth2.beacon.tools.builder.initializer import load_genesis_key_map
from eth2.beacon.tools.misc.ssz_vector import override_lengths
from eth2.beacon.types.states import BeaconState
from eth2.beacon.typing import (
    Timestamp,
)
from eth2.configs import (
    Eth2Config,
)
import p2p.ecies as ecies
from p2p.kademlia import (
    Node as KademliaNode,
)
from ssz.tools.parse import from_formatted_dict

from trinity._utils.chains import (
    construct_trinity_config_params,
    get_data_dir_for_network_id,
    get_database_socket_path,
    get_jsonrpc_socket_path,
    get_nodekey_path,
    load_nodekey,
)
from trinity._utils.eip1085 import (
    Account,
    GenesisData,
    GenesisParams,
    MiningMethod,
    extract_genesis_data,
)
from trinity._utils.filesystem import (
    PidFile,
)
from trinity._utils.version import construct_trinity_client_identifier
from trinity._utils.xdg import (
    get_xdg_trinity_root,
)
from trinity.constants import (
    ASSETS_DIR,
    DEFAULT_PREFERRED_NODES,
    IPC_DIR,
    LOG_DIR,
    LOG_FILE,
    NODE_DB_DIR,
    PID_DIR,
    SYNC_LIGHT,
    APP_IDENTIFIER_VALIDATOR_CLIENT,
)
from trinity.network_configurations import (
    PRECONFIGURED_NETWORKS
)

from eth2.beacon.chains.base import BeaconChain
from eth2.beacon.chains.testnet.medalla import BeaconChain as MedallaChain
from eth2.beacon.chains.abc import BaseBeaconChain as ABCBaseBeaconChain
from eth2.beacon.db.abc import BaseBeaconChainDB
from eth2.beacon.db.chain2 import BeaconChainDB as MedallaChainDB
from eth2.beacon.state_machines.forks.medalla.configs import MEDALLA_CONFIG
from eth2.beacon.state_machines.forks.serenity.configs import SERENITY_CONFIG


if TYPE_CHECKING:
    # avoid circular import
    from trinity.nodes.base import Node  # noqa: F401
    from trinity.chains.full import FullChain  # noqa: F401
    from trinity.chains.light import LightDispatchChain  # noqa: F401

    from eth2.beacon.chains.base import BaseBeaconChain  # noqa: F401

DATABASE_DIR_NAME = 'chain'


def _get_assets_path(network_id: int) -> Path:
    if network_id not in PRECONFIGURED_NETWORKS:
        raise TypeError(f"Unknown or unsupported `network_id`: {network_id}")
    else:
        return ASSETS_DIR / 'eip1085' / PRECONFIGURED_NETWORKS[network_id].eip1085_filename


def _load_preconfigured_genesis_config(network_id: int) -> Dict[str, Any]:
    if network_id not in PRECONFIGURED_NETWORKS:
        raise TypeError(f"Unknown or unsupported `network_id`: {network_id}")
    else:
        try:
            with _get_assets_path(network_id).open('r') as genesis_file:
                return json.load(genesis_file)
        except FileNotFoundError:
            # NOTE: this code path exists to shim eth2 into eth1
            return {}


def _get_preconfigured_chain_name(network_id: int) -> str:
    if network_id not in PRECONFIGURED_NETWORKS:
        raise TypeError(f"Unknown or unsupported `network_id`: {network_id}")
    else:
        return PRECONFIGURED_NETWORKS[network_id].chain_name


def _get_eth2_genesis_config_file_path(profile: str) -> Path:
    return ASSETS_DIR / 'eth2' / profile / 'genesis_config.json'


class Eth1ChainConfig:
    def __init__(self,
                 genesis_data: GenesisData,
                 chain_name: str = None) -> None:

        self.genesis_data = genesis_data
        self._chain_name = chain_name

    @property
    def chain_name(self) -> str:
        if self._chain_name is None:
            return "CustomChain"
        else:
            return self._chain_name

    @property
    def mining_method(self) -> MiningMethod:
        return self.genesis_data.mining_method

    def apply_consensus_engine(self, vms: VMConfiguration) -> VMConfiguration:
        if self.mining_method == MiningMethod.Clique:
            return CliqueApplier().amend_vm_configuration(vms)
        elif self.mining_method == MiningMethod.NoProof:
            return ConsensusApplier(NoProofConsensus).amend_vm_configuration(vms)
        elif self.mining_method == MiningMethod.Ethash:
            return ConsensusApplier(PowConsensus).amend_vm_configuration(vms)
        else:
            raise NotImplementedError(f"{self.mining_method} is not supported")

    @property
    def consensus_context_class(self) -> Type[ConsensusContextAPI]:
        if self.mining_method == MiningMethod.Clique:
            return CliqueConsensusContext
        else:
            return ConsensusContext

    @property
    def full_chain_class(self) -> Type['FullChain']:
        from trinity.chains.full import FullChain  # noqa: F811

        return FullChain.configure(
            __name__=self.chain_name,
            vm_configuration=self.vm_configuration,
            consensus_context_class=self.consensus_context_class,
            chain_id=self.chain_id,
        )

    @property
    def light_chain_class(self) -> Type['LightDispatchChain']:
        from trinity.chains.light import LightDispatchChain  # noqa: F811

        return LightDispatchChain.configure(
            __name__=self.chain_name,
            vm_configuration=self.vm_configuration,
            consensus_context_class=self.consensus_context_class,
            chain_id=self.chain_id,
        )

    @classmethod
    def from_eip1085_genesis_config(cls,
                                    genesis_config: Dict[str, Any],
                                    chain_name: str = None,
                                    ) -> 'Eth1ChainConfig':
        genesis_data = extract_genesis_data(genesis_config)
        return cls(
            genesis_data=genesis_data,
            chain_name=chain_name,
        )

    @classmethod
    def from_preconfigured_network(cls,
                                   network_id: int) -> 'Eth1ChainConfig':
        genesis_config = _load_preconfigured_genesis_config(network_id)
        chain_name = _get_preconfigured_chain_name(network_id)
        return cls.from_eip1085_genesis_config(genesis_config, chain_name)

    @property
    def chain_id(self) -> int:
        return self.genesis_data.chain_id

    @property
    def genesis_params(self) -> GenesisParams:
        """
        Return the genesis configuation parsed from the genesis configuration file.
        """
        return self.genesis_data.params

    @property
    def genesis_state(self) -> Dict[Address, Account]:
        return self.genesis_data.state

    def initialize_chain(self,
                         base_db: AtomicDatabaseAPI) -> 'FullChain':
        genesis_params = self.genesis_params.to_dict()
        genesis_state = {
            address: account.to_dict()
            for address, account
            in self.genesis_state.items()
        }
        return cast('FullChain', self.full_chain_class.from_genesis(
            base_db=base_db,
            genesis_params=genesis_params,
            genesis_state=genesis_state,
        ))

    @property
    def vm_configuration(self) -> VMConfiguration:
        """
        Return the vm configuration specifed from the genesis configuration file.
        """
        return self.apply_consensus_engine(self.genesis_data.vm_configuration)


LOGGING_IPC_SOCKET_FILENAME = 'logging.ipc'


TAppConfig = TypeVar('TAppConfig', bound='BaseAppConfig')


class TrinityConfig:
    """
    The :class:`~trinity.config.TrinityConfig` holds all base configurations that are generic
    enough to be shared across the different runtime modes that are available. It also gives access
    to the more specific application configurations derived from
    :class:`~trinity.config.BaseAppConfig`.

    This API is exposed to :class:`~trinity.extensibility.component.BaseComponent`
    """

    _trinity_root_dir: Path = None

    _chain_config: Eth1ChainConfig = None

    _data_dir: Path = None
    _nodekey_path: Path = None
    _logfile_path: Path = None
    _nodekey = None
    _network_id: int = None

    port: int = None
    preferred_nodes: Tuple[KademliaNode, ...] = None

    bootstrap_nodes: Tuple[KademliaNode, ...] = None

    _genesis_config: Dict[str, Any] = None

    _app_configs: Dict[Type['BaseAppConfig'], 'BaseAppConfig'] = None

    def __init__(self,
                 network_id: int,
                 app_identifier: str = "",
                 genesis_config: Dict[str, Any] = None,
                 max_peers: int = 25,
                 trinity_root_dir: Path = None,
                 trinity_tmp_root_dir: bool = False,
                 data_dir: Path = None,
                 nodekey_path: Path = None,
                 nodekey: PrivateKey = None,
                 port: int = 30303,
                 preferred_nodes: Tuple[KademliaNode, ...] = None,
                 bootstrap_nodes: Tuple[KademliaNode, ...] = None) -> None:
        self.app_identifier = app_identifier
        self.network_id = network_id
        self.max_peers = max_peers
        self.port = port
        self._app_configs = {}

        if genesis_config is not None:
            self.genesis_config = genesis_config
        elif network_id in PRECONFIGURED_NETWORKS:
            self.genesis_config = _load_preconfigured_genesis_config(network_id)
        else:
            raise TypeError(
                "No `genesis_config` was provided and the `network_id` is not "
                "in the known preconfigured networks.  Cannot initialize "
                "ChainConfig"
            )

        if trinity_root_dir is not None:
            self.trinity_root_dir = trinity_root_dir
        self.trinity_tmp_root_dir = trinity_tmp_root_dir

        if not preferred_nodes and self.network_id in DEFAULT_PREFERRED_NODES:
            self.preferred_nodes = DEFAULT_PREFERRED_NODES[self.network_id]
        else:
            self.preferred_nodes = preferred_nodes

        if bootstrap_nodes is None:
            if self.network_id in PRECONFIGURED_NETWORKS:
                bootnodes = PRECONFIGURED_NETWORKS[self.network_id].bootnodes
                self.bootstrap_nodes = tuple(
                    KademliaNode.from_uri(enode) for enode in bootnodes
                )
            else:
                self.bootstrap_nodes = tuple()
        else:
            self.bootstrap_nodes = bootstrap_nodes

        if data_dir is not None:
            self.data_dir = data_dir

        if nodekey is not None and nodekey_path is not None:
            raise ValueError("It is invalid to provide both a `nodekey` and a `nodekey_path`")
        elif nodekey_path is not None:
            self.nodekey_path = nodekey_path
        elif nodekey is not None:
            self.nodekey = nodekey

    @property
    def app_suffix(self) -> str:
        """
        Return the suffix that Trinity uses to derive various application directories depending
        on the current mode of operation (e.g. ``eth1`` or ``beacon`` to derive
        ``<trinity-root-dir>/mainnet/logs-eth1`` vs ``<trinity-root-dir>/mainnet/logs-beacon``)
        """
        return "" if len(self.app_identifier) == 0 else f"-{self.app_identifier}"

    @property
    def logfile_path(self) -> Path:
        """
        Return the path to the log file.
        """
        return self.log_dir / LOG_FILE

    @property
    def log_dir(self) -> Path:
        """
        Return the path of the directory where all log files are stored.
        """
        return self.with_app_suffix(self.data_dir / LOG_DIR)

    @property
    def trinity_root_dir(self) -> Path:
        """
        Base directory that all trinity data is stored under.

        The default ``data_dir`` path will be resolved relative to this
        directory.
        """
        if self._trinity_root_dir is not None:
            return self._trinity_root_dir
        else:
            return get_xdg_trinity_root()

    @trinity_root_dir.setter
    def trinity_root_dir(self, value: str) -> None:
        self._trinity_root_dir = Path(value).resolve()

    @property
    def data_dir(self) -> Path:
        """
        The data_dir is the base directory that all chain specific information
        for a given chain is stored.

        All defaults for chain directories are resolved relative to this
        directory.
        """
        if self._data_dir is not None:
            return self._data_dir
        else:
            return get_data_dir_for_network_id(self.network_id, self.trinity_root_dir)

    @data_dir.setter
    def data_dir(self, value: str) -> None:
        self._data_dir = Path(value).resolve()

    @property
    def database_ipc_path(self) -> Path:
        """
        Return the path for the database IPC socket connection.
        """
        return get_database_socket_path(self.ipc_dir)

    @property
    def node_db_dir(self) -> Path:
        """
        Return the directory for the Node database.
        """
        return self.with_app_suffix(self.data_dir / NODE_DB_DIR)

    @property
    def logging_ipc_path(self) -> Path:
        """
        Return the path for the logging IPC socket connection.
        """
        return self.ipc_dir / LOGGING_IPC_SOCKET_FILENAME

    @property
    def ipc_dir(self) -> Path:
        """
        Return the base directory for all open IPC files.
        """
        return self.with_app_suffix(self.data_dir / IPC_DIR)

    @property
    def pid_dir(self) -> Path:
        """
        Return the base directory for all PID files.
        """
        return self.with_app_suffix(self.data_dir / PID_DIR)

    @property
    def jsonrpc_ipc_path(self) -> Path:
        """
        Return the path for the JSON-RPC server IPC socket.
        """
        return get_jsonrpc_socket_path(self.ipc_dir)

    @property
    def nodekey_path(self) -> Path:
        """
        Path where the nodekey is stored
        """
        if self._nodekey_path is None:
            if self._nodekey is not None:
                return None
            else:
                return get_nodekey_path(self.data_dir)
        else:
            return self._nodekey_path

    @nodekey_path.setter
    def nodekey_path(self, value: str) -> None:
        self._nodekey_path = Path(value).resolve()

    @property
    def nodekey(self) -> PrivateKey:
        """
        The :class:`~eth_keys.datatypes.PrivateKey` which trinity uses to derive the
        public key needed to identify itself on the network.
        """
        if self._nodekey is None:
            try:
                return load_nodekey(self.nodekey_path)
            except FileNotFoundError:
                # no file at the nodekey_path so we have a null nodekey
                return None
        else:
            if isinstance(self._nodekey, bytes):
                return keys.PrivateKey(self._nodekey)
            elif isinstance(self._nodekey, PrivateKey):
                return self._nodekey
            return self._nodekey

    @nodekey.setter
    def nodekey(self, value: Union[bytes, PrivateKey]) -> None:
        if isinstance(value, bytes):
            self._nodekey = keys.PrivateKey(value)
        elif isinstance(value, PrivateKey):
            self._nodekey = value
        else:
            raise TypeError(
                "Nodekey must either be a raw byte-string or an eth_keys "
                f"`PrivateKey` instance: got {type(self._nodekey)}"
            )

    @contextmanager
    def process_id_file(self, process_name: str):  # type: ignore
        """
        Context manager API to generate process identification files (pid) in the current
        :meth:`pid_dir`.

        .. code-block:: python

            trinity_config.process_id_file('networking'):
                ... # pid file sitting in pid directory while process is running
            ... # pid file cleaned up
        """
        with PidFile(process_name, self.pid_dir):
            yield

    @classmethod
    def from_parser_args(cls,
                         parser_args: argparse.Namespace,
                         app_identifier: str,
                         app_config_types: Iterable[Type['BaseAppConfig']]) -> 'TrinityConfig':
        """
        Initialize a :class:`~trinity.config.TrinityConfig` from the namespace object produced by
        an :class:`~argparse.ArgumentParser`.
        """
        constructor_kwargs = construct_trinity_config_params(parser_args)
        trinity_config = cls(app_identifier=app_identifier, **constructor_kwargs)

        trinity_config.initialize_app_configs(parser_args, app_config_types)

        return trinity_config

    def initialize_app_configs(self,
                               parser_args: argparse.Namespace,
                               app_config_types: Iterable[Type['BaseAppConfig']]) -> None:
        """
        Initialize :class:`~trinity.config.BaseAppConfig` instances for the passed
        ``app_config_types`` based on the ``parser_args`` and the existing
        :class:`~trinity.config.TrintiyConfig` instance.
        """
        for app_config_type in app_config_types:
            self.add_app_config(app_config_type.from_parser_args(parser_args, self))

    def add_app_config(self, app_config: 'BaseAppConfig') -> None:
        """
        Register the given ``app_config``.
        """
        self._app_configs[type(app_config)] = app_config

    def has_app_config(self, app_config_type: Type['BaseAppConfig']) -> bool:
        """
        Check if a :class:`~trinity.config.BaseAppConfig` instance exists that matches the given
        ``app_config_type``.
        """
        return app_config_type in self._app_configs.keys()

    def get_app_config(self, app_config_type: Type[TAppConfig]) -> TAppConfig:
        """
        Return the registered :class:`~trinity.config.BaseAppConfig` instance that matches
        the given ``app_config_type``.
        """
        # We want this API to return the specific type of the app config that is requested.
        # Our backing field only knows that it is holding `BaseAppConfig`'s but not concrete types
        return cast(TAppConfig, self._app_configs[app_config_type])

    def with_app_suffix(self, path: Path) -> Path:
        """
        Return a :class:`~pathlib.Path` that matches the given ``path`` plus the :meth:`app_suffix`
        """
        return path.with_name(path.name + self.app_suffix)


class BaseAppConfig(ABC):

    def __init__(self, trinity_config: TrinityConfig):
        self.trinity_config = trinity_config

    @classmethod
    @abstractmethod
    def from_parser_args(cls,
                         args: argparse.Namespace,
                         trinity_config: TrinityConfig) -> 'BaseAppConfig':
        """
        Initialize from the namespace object produced by
        an ``argparse.ArgumentParser`` and the :class:`~trinity.config.TrinityConfig`
        """
        pass


class BaseEth2AppConfig(BaseAppConfig):
    pass


class Eth1DbMode(Enum):

    FULL = auto()
    LIGHT = auto()


class Eth1AppConfig(BaseAppConfig):

    def __init__(self, trinity_config: TrinityConfig, sync_mode: str):
        super().__init__(trinity_config)
        self.trinity_config = trinity_config
        self._sync_mode = sync_mode

    @classmethod
    def from_parser_args(cls,
                         args: argparse.Namespace,
                         trinity_config: TrinityConfig) -> 'BaseAppConfig':
        """
        Initialize from the namespace object produced by
        an ``argparse.ArgumentParser`` and the :class:`~trinity.config.TrinityConfig`
        """
        return cls(trinity_config, args.sync_mode)

    @property
    def database_dir(self) -> Path:
        """
        Path where the chain database is stored.

        This is resolved relative to the ``data_dir``
        """
        path = self.trinity_config.data_dir / DATABASE_DIR_NAME
        if self.database_mode is Eth1DbMode.LIGHT:
            return self.trinity_config.with_app_suffix(path) / "light"
        elif self.database_mode is Eth1DbMode.FULL:
            return self.trinity_config.with_app_suffix(path) / "full"
        else:
            raise Exception(f"Unsupported Database Mode: {self.database_mode}")

    @property
    def database_mode(self) -> Eth1DbMode:
        """
        Return the :class:`~trinity.config.Eth1DbMode` for the currently used database
        """
        if self.sync_mode == SYNC_LIGHT:
            return Eth1DbMode.LIGHT
        else:
            return Eth1DbMode.FULL

    def get_chain_config(self) -> Eth1ChainConfig:
        """
        Return the :class:`~trinity.config.Eth1ChainConfig` either derived from the ``network_id``
        or a custom genesis file.
        """
        # the `ChainConfig` object cannot be pickled so we can't cache this
        # value since the TrinityConfig is sent across process boundaries.
        if self.trinity_config.network_id in PRECONFIGURED_NETWORKS:
            return Eth1ChainConfig.from_preconfigured_network(self.trinity_config.network_id)
        else:
            return Eth1ChainConfig.from_eip1085_genesis_config(self.trinity_config.genesis_config)

    @property
    def node_class(self) -> Type['Node[Any]']:
        """
        Return the ``Node`` class that trinity uses.
        """
        from trinity.nodes.full import FullNode
        from trinity.nodes.light import LightNode

        if self.database_mode is Eth1DbMode.FULL:
            return FullNode
        elif self.database_mode is Eth1DbMode.LIGHT:
            return LightNode
        else:
            raise NotImplementedError(f"Database mode {self.database_mode} not supported")

    @property
    def sync_mode(self) -> str:
        """
        Return the currently used sync mode
        """
        return self._sync_mode


class BeaconChainConfig:
    _genesis_state: BeaconState
    _beacon_chain_class: Type['BaseBeaconChain'] = None
    _genesis_config: Eth2Config = None
    _key_map: Dict[BLSPubkey, int]

    def __init__(self,
                 genesis_state: BeaconState,
                 genesis_config: Eth2Config,
                 genesis_validator_key_map: Dict[BLSPubkey, int],
                 beacon_chain_class: Type['BaseBeaconChain'] = SkeletonLakeChain) -> None:
        self._genesis_state = genesis_state
        self._eth2_config = genesis_config
        self._key_map = genesis_validator_key_map
        self._beacon_chain_class = beacon_chain_class

    @property
    def genesis_config(self) -> Eth2Config:
        """
        NOTE: this ``genesis_config`` means something slightly different from the
        genesis config referenced in other places in this class...
        TODO:(ralexstokes) patch up the names here...
        """
        return self._eth2_config

    @property
    def genesis_time(self) -> Timestamp:
        return self._genesis_state.genesis_time

    @property
    def beacon_chain_class(self) -> Type['BaseBeaconChain']:
        return self._beacon_chain_class

    @classmethod
    def from_genesis_config(cls, config_profile: str) -> 'BeaconChainConfig':
        """
        Construct an instance of ``cls`` reading the genesis configuration
        data under the local data directory.
        """
        if config_profile == "mainnet":
            beacon_chain_class = BeaconChain
        else:
            beacon_chain_class = SkeletonLakeChain

        try:
            with open(_get_eth2_genesis_config_file_path(config_profile)) as config_file:
                genesis_config = json.load(config_file)
        except FileNotFoundError as e:
            raise Exception("unable to load genesis config: %s", e)

        eth2_config = Eth2Config.from_formatted_dict(genesis_config["eth2_config"])
        # NOTE: have to ``override_lengths`` before we can parse the ``BeaconState``
        override_lengths(eth2_config)

        genesis_state = from_formatted_dict(genesis_config["genesis_state"], BeaconState)
        genesis_validator_key_map = load_genesis_key_map(
            genesis_config["genesis_validator_key_pairs"]
        )
        return cls(
            genesis_state,
            eth2_config,
            genesis_validator_key_map,
            beacon_chain_class=beacon_chain_class
        )

    @classmethod
    def get_genesis_config_file_path(cls, profile: str) -> Path:
        return _get_eth2_genesis_config_file_path(profile)

    def initialize_chain(self,
                         base_db: AtomicDatabaseAPI) -> 'BaseBeaconChain':
        return self.beacon_chain_class.from_genesis(
            base_db=base_db,
            genesis_state=self._genesis_state,
        )


class BeaconAppConfig(BaseEth2AppConfig):
    def __init__(
        self,
        config: TrinityConfig,
        p2p_maddr: Multiaddr,
        orchestration_profile: str,
        bootstrap_nodes: Tuple[Multiaddr, ...] = (),
        preferred_nodes: Tuple[Multiaddr, ...] = (),
    ) -> None:
        super().__init__(config)
        self.p2p_maddr = p2p_maddr
        self.bootstrap_nodes = config.bootstrap_nodes
        self.preferred_nodes = config.preferred_nodes
        self.client_identifier = construct_trinity_client_identifier()
        self.orchestration_profile = orchestration_profile

    @classmethod
    def from_parser_args(
        cls, args: argparse.Namespace, trinity_config: TrinityConfig
    ) -> "BaseAppConfig":
        """
        Initialize from the namespace object produced by
        an ``argparse.ArgumentParser`` and the :class:`~trinity.config.TrinityConfig`
        """
        bootstrap_nodes = args.bootstrap_nodes
        preferred_nodes = args.preferred_nodes
        p2p_maddr = args.p2p_maddr

        return cls(
            trinity_config,
            p2p_maddr,
            args.orchestration_profile,
            bootstrap_nodes,
            preferred_nodes
        )

    @property
    def database_dir(self) -> Path:
        """
        Return the path where the chain database is stored.

        This is resolved relative to the ``data_dir``
        """
        path = self.trinity_config.data_dir / DATABASE_DIR_NAME
        return self.trinity_config.with_app_suffix(path) / f"full_{self.orchestration_profile}"

    def get_chain_config(self, config_profile: str = "minimal") -> BeaconChainConfig:
        """
        Return the :class:`~trinity.config.BeaconChainConfig` that is derived from the genesis file
        """
        return BeaconChainConfig.from_genesis_config(config_profile)


class BeaconTrioChainConfig(ABC):
    chain_db_class: Type[BaseBeaconChainDB]
    chain_class: Type[ABCBaseBeaconChain]


class MedallaChainConfig(BeaconTrioChainConfig):
    chain_db_class: Type[BaseBeaconChainDB] = MedallaChainDB
    chain_class: Type[ABCBaseBeaconChain] = MedallaChain


class MainnetChainConfig(BeaconTrioChainConfig):
    # TODO: update to mainnet instances
    chain_db_class: Type[BaseBeaconChainDB] = MedallaChainDB
    chain_class: Type[ABCBaseBeaconChain] = MedallaChain


ETH2_NETWORKS = {
    "medalla": MEDALLA_CONFIG,
    "mainnet": SERENITY_CONFIG,
}

ETH2_CHAIN_CONFIGS = {
    "medalla": MedallaChainConfig,
    "mainnet": MainnetChainConfig,
}


def _load_predefined_eth2_network(network: str) -> Eth2Config:
    """
    NOTE: ``network`` should exist given the argparse configuration
    which references ``ETH2_NETWORKS``.
    """
    return ETH2_NETWORKS[network]


def _load_eth2_network_from_yaml(network_config_path: str) -> Eth2Config:
    raise NotImplementedError("loading network from YAML config is unsupported")


def _resolve_node_key(trinity_config: TrinityConfig, nodekey_seed: Optional[str]) -> PrivateKey:
    if nodekey_seed:
        private_key_bytes = hashlib.sha256(nodekey_seed.encode()).digest()
        nodekey = PrivateKey(private_key_bytes)
        trinity_config.nodekey = nodekey
        return

    if trinity_config.nodekey is None:
        nodekey = ecies.generate_privkey()
        with open(trinity_config.nodekey_path, 'wb') as nodekey_file:
            nodekey_file.write(nodekey.to_bytes())
        trinity_config.nodekey = nodekey


class BeaconTrioAppConfig(BaseEth2AppConfig):
    def __init__(
        self,
        config: TrinityConfig,
        network_name: str,
        network_config: Eth2Config,
        p2p_maddr: Multiaddr,
        validator_api_port: int,
        bootstrap_nodes: Tuple[Multiaddr, ...] = (),
        preferred_nodes: Tuple[Multiaddr, ...] = (),
        genesis_state_ssz: Optional[Path] = None,
    ) -> None:
        super().__init__(config)
        self.p2p_maddr = p2p_maddr
        self.network_name = network_name
        self.network_config = network_config
        self.validator_api_port = validator_api_port
        self.bootstrap_nodes = bootstrap_nodes
        self.preferred_nodes = preferred_nodes
        self.client_identifier = construct_trinity_client_identifier()
        self.genesis_state_ssz = genesis_state_ssz

    @classmethod
    def from_parser_args(
        cls, args: argparse.Namespace, trinity_config: TrinityConfig
    ) -> "BaseAppConfig":
        """
        Initialize from the namespace object produced by
        an ``argparse.ArgumentParser`` and the :class:`~trinity.config.TrinityConfig`
        """
        # NOTE: ``args.network`` has a default so check for an override in
        # the ``network_config`` first...
        if args.network_config:
            network_config = _load_eth2_network_from_yaml(args.network_config)
            network_name = "custom network"
        else:
            network_config = _load_predefined_eth2_network(args.network)
            network_name = args.network

        # NOTE: required for SSZ types to work correctly...
        override_lengths(network_config)

        p2p_maddr = args.p2p_maddr
        bootstrap_nodes = args.bootstrap_maddrs
        preferred_nodes = args.preferred_maddrs
        validator_api_port = args.validator_api_port
        genesis_state_ssz = args.genesis_state_ssz

        _resolve_node_key(trinity_config, args.nodekey_seed)

        return cls(
            trinity_config,
            network_name,
            network_config,
            p2p_maddr,
            validator_api_port,
            bootstrap_nodes,
            preferred_nodes,
            genesis_state_ssz,
        )

    @property
    def database_dir(self) -> Path:
        """
        Return the path where the chain database is stored.

        This is resolved relative to the ``data_dir``
        """
        root_dir = self.trinity_config.trinity_root_dir
        path = self.trinity_config.with_app_suffix(root_dir)
        network_identifier = _build_network_identifier(self.network_config)
        peer_identifier = self.trinity_config.nodekey.to_hex()[2:]
        return path / network_identifier / peer_identifier

    def get_chain_config(self) -> BeaconTrioChainConfig:
        if self.network_name in ETH2_CHAIN_CONFIGS:
            return ETH2_CHAIN_CONFIGS[self.network_name]()

        raise Exception(
            f"unknown chain config for network name {self.network_name}."
            " please define before use."
        )


def _build_network_identifier(config: Eth2Config) -> str:
    return (
        f"{config.DEPOSIT_CHAIN_ID}"
        f"{config.DEPOSIT_NETWORK_ID}"
        f"{encode_hex(config.DEPOSIT_CONTRACT_ADDRESS)[2:]}"
    )


class ValidatorClientAppConfig(BaseEth2AppConfig):
    @classmethod
    def from_parser_args(cls,
                         args: argparse.Namespace,
                         trinity_config: TrinityConfig) -> 'BaseAppConfig':
        """
        Initialize from the namespace object produced by
        an ``argparse.ArgumentParser`` and the :class:`~trinity.config.TrinityConfig`
        """
        return cls(trinity_config)

    @property
    def root_dir(self) -> Path:
        """
        Return the root directory of the validator client data.

        This is resolved relative to the ``data_dir``
        """
        return self.trinity_config.data_dir / APP_IDENTIFIER_VALIDATOR_CLIENT

    @property
    def genesis_config_path(self) -> Path:
        return _get_eth2_genesis_config_file_path("minimal")
