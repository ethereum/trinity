from dataclasses import dataclass

from lahja import BaseEvent


@dataclass
class UPnPMapping(BaseEvent):
    ip: str
