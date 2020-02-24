from dataclasses import dataclass

from lahja import BaseEvent


@dataclass
class NewUPnPMapping(BaseEvent):
    ip: str
