from trinity.protocol.eth.monitors import ETHChainTipMonitor
from trinity.protocol.eth.peer import ETHProxyPeer
from trinity.sync.common.headers import BaseHeaderChainSyncer


class ETHHeaderChainSyncer(BaseHeaderChainSyncer[ETHProxyPeer]):
    tip_monitor_class = ETHChainTipMonitor
