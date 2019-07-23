from trinity.protocol.common.monitors import BaseChainTipMonitor
from trinity.protocol.les.events import AnnounceEvent
from trinity.protocol.les.peer import LESProxyPeer


class LightChainTipMonitor(BaseChainTipMonitor[LESProxyPeer]):
    monitor_event_type = AnnounceEvent
