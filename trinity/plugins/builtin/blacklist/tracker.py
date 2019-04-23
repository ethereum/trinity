import datetime
import math
from pathlib import Path

from sqlalchemy import (
    Column,
    Integer,
    DateTime,
    String,
)
from sqlalchemy.orm import (
    Session as BaseSession,
)
from sqlalchemy.orm.exc import (
    NoResultFound,
)

from lahja import (
    BroadcastConfig,
    Endpoint,
)

from eth_utils import humanize_seconds

from p2p.kademlia import Node
from p2p.tracking.connection import BaseConnectionTracker

from trinity.constants import (
    BLACKLIST_EVENTBUS_ENDPOINT,
)

from trinity.db.orm import (
    Base,
    get_tracking_database,
)
from .events import (
    BlacklistEvent,
    ShouldConnectToPeerRequest,
)


class BlacklistRecord(Base):
    __tablename__ = 'blacklist_records'

    id = Column(Integer, primary_key=True)
    uri = Column(String, unique=True, nullable=False, index=True)
    expires_at = Column(DateTime(timezone=True), nullable=False)
    reason = Column(String, nullable=False)
    error_count = Column(Integer, default=1, nullable=False)

    def is_expired(self) -> bool:
        return datetime.datetime.utcnow() < self.expires_at


class SQLiteConnectionTracker(BaseConnectionTracker):
    def __init__(self, session: BaseSession):
        self.session = session

    #
    # Core API
    #
    def record_blacklist(self, remote: Node, timeout: int, reason: str) -> None:
        if self._record_exists(remote.uri()):
            self._update_record(remote, timeout, reason)
        else:
            self._create_record(remote, timeout, reason)

    def should_connect_to(self, remote: Node) -> bool:
        try:
            record = self._get_record(remote.uri())
        except NoResultFound:
            return True

        now = datetime.datetime.utcnow()
        if now < record.expires_at:
            delta = record.expires_at - now
            self.logger.debug(
                'skipping %s, it failed because "%s" and is not usable for %s',
                remote,
                record.reason,
                humanize_seconds(delta.total_seconds()),
            )
            return False

        return True

    #
    # Helpers
    #
    def _get_record(self, uri: str) -> BlacklistRecord:
        return self.session.query(BlacklistRecord).filter_by(uri=uri).one()

    def _record_exists(self, uri: str) -> bool:
        try:
            self._get_record(uri)
        except NoResultFound:
            return False
        else:
            return True

    def _create_record(self, remote: Node, timeout: int, reason: str) -> None:
        uri = remote.uri()
        expires_at = datetime.datetime.utcnow() + datetime.timedelta(seconds=timeout)

        record = BlacklistRecord(uri=uri, expires_at=expires_at, reason=reason)
        self.session.add(record)
        self.session.commit()

        self.logger.debug(
            '%s will not be retried for %s because %s',
            remote,
            humanize_seconds(timeout),
            reason,
        )

    def _update_record(self, remote: Node, timeout: int, reason: str) -> None:
        uri = remote.uri()
        record = self._get_record(uri)
        record.error_count += 1

        adjusted_timeout = int(timeout * math.sqrt(record.error_count))
        record.expires_at += datetime.timedelta(seconds=adjusted_timeout)
        record.reason = reason
        record.error_count += 1

        self.session.add(record)
        self.session.commit()

        self.logger.debug(
            '%s will not be retried for %s because %s',
            remote,
            humanize_seconds(adjusted_timeout),
            reason,
        )


class MemoryConnectionTracker(SQLiteConnectionTracker):
    def __init__(self) -> None:
        session = get_tracking_database(Path(":memory:"))
        super().__init__(session)


TO_BLACKLIST_BROADCAST_CONFIG = BroadcastConfig(filter_endpoint=BLACKLIST_EVENTBUS_ENDPOINT)


class EventBusConnectionTracker(BaseConnectionTracker):
    def __init__(self,
                 event_bus: Endpoint,
                 config: BroadcastConfig = TO_BLACKLIST_BROADCAST_CONFIG) -> None:
        self.event_bus = event_bus
        self.config = config

    def record_blacklist(self, remote: Node, timeout: int, reason: str) -> None:
        self.event_bus.broadcast(
            BlacklistEvent(remote, timeout, reason=reason),
            self.config,
        )

    def should_connect_to(self, remote: Node) -> bool:
        raise NotImplementedError("Must use coro_should_connect_to")

    async def coro_should_connect_to(self, remote: Node) -> bool:
        self.logger.debug('sending should connect request: %s', remote)
        response = await self.event_bus.request(
            ShouldConnectToPeerRequest(remote),
            self.config
        )
        self.logger.debug('got should connect response')
        return response.should_connect
