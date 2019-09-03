import factory

from p2p.manager import PoolManager

from .cancel_token import CancelTokenFactory
from .keys import PrivateKeyFactory
from .p2p_proto import DevP2PHandshakeParamsFactory
from .pool import ConnectionPoolFactory


class PoolManager(factory.Factory):
    class Meta:
        model = PoolManager

    pool = factory.SubFactory(ConnectionPoolFactory)
    private_key = factory.SubFactory(PrivateKeyFactory)
    p2p_handshake_params = factory.SubFactory(DevP2PHandshakeParamsFactory)
    handshaker_providers = ()
    token = factory.SubFactory(CancelTokenFactory)
