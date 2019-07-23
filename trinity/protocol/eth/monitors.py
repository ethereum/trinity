from trinity.protocol.common.monitors import BaseChainTipMonitor
from trinity.protocol.eth.peer import ETHProxyPeer
from trinity.protocol.eth.events import NewBlockEvent


class ETHChainTipMonitor(BaseChainTipMonitor[ETHProxyPeer]):

    monitor_event_type = NewBlockEvent
