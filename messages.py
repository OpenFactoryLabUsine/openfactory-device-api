import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any

from models import Variable


@dataclass
class ConnectionEstablishedMessage:
    asset_uuid: str
    variables: dict[str, Any]
    connection_count: int
    event: str = "connection_established"
    timestamp: str = field(default_factory=lambda: datetime.now(UTC).isoformat())

    def to_json(self) -> str:
        return json.dumps(asdict(self))


@dataclass
class PingMessage:
    active_equipments: int = 0
    event: str = "ping"
    timestamp: str = field(default_factory=lambda: datetime.now(UTC).isoformat())

    def to_json(self) -> str:
        return json.dumps(asdict(self))


@dataclass
class ErrorMessage:
    message: str
    event: str = "error"
    timestamp: str = field(default_factory=lambda: datetime.now(UTC).isoformat())

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
    timestamp: str = field(default_factory=lambda: datetime.now(UTC).isoformat())

    def to_json(self) -> str:
        return json.dumps(asdict(self))


@dataclass
class DevicesListMessage:
    equipments: list
    event: str = "equipments_list"
    timestamp: str = field(default_factory=lambda: datetime.now(UTC).isoformat())

    def to_json(self) -> str:
        return json.dumps(asdict(self))
    

@dataclass
class DeviceUpdateMessage:
    asset_uuid: str
    variables: list[Variable]
    event: str = "equipment_update"

    def to_dict(self) -> dict:
        return {
            "event": self.event,
            "asset_uuid": self.asset_uuid,
            "variables": [asdict(variable) for variable in self.variables],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict())