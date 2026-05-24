import json
import time
from dataclasses import asdict, dataclass, field
from typing import Any

from models import Variable


@dataclass
class ConnectionEstablishedMessage:
    asset_uuid: str
    variables: dict[str, Any]
    connection_count: int
    event: str = "connection_established"
    timestamp: float = field(default_factory=time.time)

    def to_json(self) -> str:
        return json.dumps(asdict(self))


@dataclass
class PingMessage:
    active_equipments: int = 0
    event: str = "ping"
    timestamp: float = field(default_factory=time.time)

    def to_json(self) -> str:
        return json.dumps(asdict(self))


@dataclass
class ErrorMessage:
    message: str
    event: str = "error"
    timestamp: float = field(default_factory=time.time)

    def to_json(self) -> str:
        return json.dumps(asdict(self))


@dataclass
class SimulationModeUpdatedMessage:
    value: bool
    success: bool = True
    error: str | None = None
    event: str = "simulation_mode_updated"

    def to_json(self) -> str:
        data = asdict(self)
        if data["error"] is None:
            del data["error"]
        return json.dumps(data)


@dataclass
class StreamDroppedMessage:
    asset_uuid: str
    success: bool = True
    event: str = "connection_dropped"
    timestamp: float = field(default_factory=time.time)

    def to_json(self) -> str:
        return json.dumps(asdict(self))


@dataclass
class DevicesListMessage:
    equipments: list
    event: str = "equipments_list"
    timestamp: float = field(default_factory=time.time)

    def to_json(self) -> str:
        return json.dumps(asdict(self))
    
@dataclass
class DeviceUpdateMessage:
    asset_uuid: str
    variables: list[Variable]
    event: str = "equipment_update"
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return {
            "event": self.event,
            "asset_uuid": self.asset_uuid,
            "timestamp": self.timestamp,
            "items": [asdict(item) for item in self.variables],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict())
