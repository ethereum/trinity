from abc import ABC, abstractmethod
import datetime
import logging
import math
from pathlib import Path
from typing import (
    Dict,
    Type,
)

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

from eth_utils import humanize_seconds

from p2p.kademlia import Node
from p2p.exceptions import (
    BaseP2PError,
    HandshakeFailure,
    TooManyPeersFailure,
    WrongNetworkFailure,
    WrongGenesisFailure,
)

from .db import (
    Base,
    get_tracking_database,
)


ONE_DAY = 60 * 60 * 24
FAILURE_TIMEOUTS: Dict[Type[Exception], int] = {
    HandshakeFailure: 10,  # 10 seconds
    WrongNetworkFailure: ONE_DAY,
    WrongGenesisFailure: ONE_DAY,
    TooManyPeersFailure: 60,  # one minute
}


def get_timeout_for_failure(failure: BaseP2PError) -> int:
    for cls in type(failure).__mro__:
        if cls in FAILURE_TIMEOUTS:
            return FAILURE_TIMEOUTS[cls]
    failure_name = type(failure).__name__
    raise Exception(f'Unknown failure type: {failure_name}')


class BaseConnectionTracker(ABC):
    @abstractmethod
    def record_failure(self, remote: Node, failure: BaseP2PError) -> None:
        pass

    @abstractmethod
    def should_connect_to(self, remote: Node) -> bool:
        pass


class NoopTracker(BaseConnectionTracker):
    def track_node(self, remote: Node, is_outbound: bool) -> None:
        pass

    def record_failure(self, remote: Node, failure: BaseP2PError) -> None:
        pass

    def should_connect_to(self, remote: Node) -> bool:
        return True


class BlacklistRecord(Base):
    __tablename__ = 'blacklist_records'

    id = Column(Integer, primary_key=True)
    uri = Column(String, unique=True, nullable=False, index=True)
    expires_at = Column(DateTime(timezone=True), nullable=False, index=True)
    reason = Column(String, nullable=False)
    error_count = Column(Integer, default=1, nullable=False)

    def is_expired(self) -> bool:
        return datetime.datetime.utcnow() < self.expires_at


class SQLiteTracker(BaseConnectionTracker):
    logger = logging.getLogger('p2p.tracking.connection.SQLiteTracker')

    def __init__(self, session: BaseSession):
        self.session = session

    #
    # Core API
    #
    def record_failure(self, remote: Node, failure: BaseP2PError) -> None:
        failure_name = type(failure).__name__

        timeout = get_timeout_for_failure(failure)

        if self._record_exists(remote.uri()):
            self._update_record(remote, timeout, failure_name)
        else:
            self._create_record(remote, timeout, failure_name)

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


class MemoryTracker(SQLiteTracker):
    def __init__(self) -> None:
        session = get_tracking_database(Path(":memory:"))
        super().__init__(session)
