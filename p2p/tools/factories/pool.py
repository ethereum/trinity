import factory

from p2p.pool import ConnectionPool


class ConnectionPoolFactory(factory.Factory):
    class Meta:
        model = ConnectionPool
