import datetime
import math
from pathlib import Path
from typing import Tuple

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
    EndpointAPI,
)

from eth_utils import humanize_seconds, to_bytes, to_hex

from p2p.abc import NodeAPI
from p2p.discv5.typing import NodeID
from p2p.tracking.connection import BaseConnectionTracker

from trinity.constants import (
    NETWORKDB_EVENTBUS_ENDPOINT,
)
from trinity.db.orm import (
    Base,
    get_tracking_database,
)
from .events import (
    BlacklistEvent,
    GetBlacklistedPeersRequest,
)


class BlacklistRecord(Base):
    __tablename__ = 'blacklist_recordsv'

    id = Column(Integer, primary_key=True)
    # The hex-encoded NodeID, with the 0x prefix
    node_id = Column(String, unique=True, nullable=False, index=True)
    expires_at = Column(DateTime(timezone=True), nullable=False)
    reason = Column(String, nullable=False)
    error_count = Column(Integer, default=1, nullable=False)


def adjust_repeat_offender_timeout(base_timeout: float, error_count: int) -> datetime.datetime:
    """
    sub-linear scaling based on number of errors recorded against the offender.
    """
    adjusted_timeout_seconds = int(base_timeout * math.sqrt(error_count + 1))
    delta = datetime.timedelta(seconds=adjusted_timeout_seconds)
    return datetime.datetime.utcnow() + delta


class SQLiteConnectionTracker(BaseConnectionTracker):
    def __init__(self, session: BaseSession):
        self.session = session

    #
    # Core API
    #
    def record_blacklist(self, remote: NodeAPI, timeout_seconds: int, reason: str) -> None:
        try:
            record = self._get_record(remote.id)
        except NoResultFound:
            expires_at = datetime.datetime.utcnow() + datetime.timedelta(seconds=timeout_seconds)
            self._create_record(remote, expires_at, reason)
        else:
            scaled_expires_at = adjust_repeat_offender_timeout(
                timeout_seconds,
                record.error_count + 1,
            )
            self._update_record(remote, scaled_expires_at, reason)

    async def get_blacklisted(self) -> Tuple[NodeID, ...]:
        now = datetime.datetime.utcnow()
        # mypy doesn't know about the type of the `query()` function
        records = self.session.query(BlacklistRecord).filter(  # type: ignore
            BlacklistRecord.expires_at > now
        )
        return tuple(NodeID(to_bytes(hexstr=record.node_id)) for record in records)

    #
    # Helpers
    #
    def _get_record(self, node_id: NodeID) -> BlacklistRecord:
        # mypy doesn't know about the type of the `query()` function
        return self.session.query(BlacklistRecord).filter_by(  # type: ignore
            node_id=to_hex(node_id)).one()

    def _record_exists(self, node_id: NodeID) -> bool:
        try:
            self._get_record(node_id)
        except NoResultFound:
            return False
        else:
            return True

    def _create_record(self, remote: NodeAPI, expires_at: datetime.datetime, reason: str) -> None:
        record = BlacklistRecord(node_id=to_hex(remote.id), expires_at=expires_at, reason=reason)
        self.session.add(record)
        # mypy doesn't know about the type of the `commit()` function
        self.session.commit()  # type: ignore

        usable_delta = expires_at - datetime.datetime.utcnow()
        self.logger.debug(
            '%s will not be retried for %s because %s',
            remote,
            humanize_seconds(usable_delta.total_seconds()),
            reason,
        )

    def _update_record(self, remote: NodeAPI, expires_at: datetime.datetime, reason: str) -> None:
        record = self._get_record(remote.id)

        if expires_at > record.expires_at:
            # only update expiration if it is further in the future than the existing expiration
            record.expires_at = expires_at
        record.reason = reason
        record.error_count += 1

        self.session.add(record)
        # mypy doesn't know about the type of the `commit()` function
        self.session.commit()  # type: ignore

        usable_delta = expires_at - datetime.datetime.utcnow()
        self.logger.debug(
            '%s will not be retried for %s because %s',
            remote,
            humanize_seconds(usable_delta.total_seconds()),
            reason,
        )


class MemoryConnectionTracker(SQLiteConnectionTracker):
    def __init__(self) -> None:
        session = get_tracking_database(Path(":memory:"))
        super().__init__(session)


TO_NETWORKDB_BROADCAST_CONFIG = BroadcastConfig(filter_endpoint=NETWORKDB_EVENTBUS_ENDPOINT)


class ConnectionTrackerClient(BaseConnectionTracker):
    def __init__(self,
                 event_bus: EndpointAPI,
                 config: BroadcastConfig = TO_NETWORKDB_BROADCAST_CONFIG) -> None:
        self.event_bus = event_bus
        self.config = config

    def record_blacklist(self, remote: NodeAPI, timeout_seconds: int, reason: str) -> None:
        self.event_bus.broadcast_nowait(
            BlacklistEvent(remote, timeout_seconds, reason=reason),
            self.config,
        )

    async def get_blacklisted(self) -> Tuple[NodeID, ...]:
        response = await self.event_bus.request(GetBlacklistedPeersRequest(), self.config)
        return response.peers
