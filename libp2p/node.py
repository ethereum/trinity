from .connmgr import (
    BaseConnectionManager,
)
from .dht import (
    BaseDHT,
)
from .host import (
    BaseHost,
)
from .pubsub import (
    BasePubSub,
)

class Node:
    """
    Reference:
        - libp2p daemon: https://github.com/libp2p/go-libp2p-daemon/blob/master/daemon.go
        - sharding-p2p-poc: https://github.com/ethresearch/sharding-p2p-poc/blob/master/node.go
    """

    host: BaseHost
    dht: BaseDHT
    pubsub: BasePubSub
    connmgr: BaseConnectionManager

