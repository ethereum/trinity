from trinity.protocol.les.monitors import LightChainTipMonitor
from trinity.protocol.les.peer import LESProxyPeer
from trinity.sync.common.headers import BaseHeaderChainSyncer


class LightHeaderChainSyncer(BaseHeaderChainSyncer[LESProxyPeer]):
    tip_monitor_class = LightChainTipMonitor
