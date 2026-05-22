import json
import time
from dataclasses import asdict, dataclass, field
from typing import Any

from models import Variable


@dataclass
class ConnectionEstablishedMessage:
    device_uuid: str
    variables: dict[str, Any]
    connection_count: int
    event: str = "connection_established"
    timestamp: float = field(default_factory=time.time)

    def to_json(self) -> str:
        return json.dumps(asdict(self))


@dataclass
class PingMessage:
    active_devices: int = 0
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
    device_uuid: str
    success: bool = True
    event: str = "stream_dropped"
    timestamp: float = field(default_factory=time.time)

    def to_json(self) -> str:
        return json.dumps(asdict(self))


@dataclass
class DevicesListMessage:
    devices: list
    event: str = "devices_list"
    timestamp: float = field(default_factory=time.time)

    def to_json(self) -> str:
        return json.dumps(asdict(self))
    
@dataclass
class DeviceUpdateMessage:
    device_uuid: str
    variables: list[Variable]
    event: str = "device_update"
    timestamp: float = field(default_factory=time.time)

    def to_json(self) -> str:
        return json.dumps({
            "event": self.event,
            "device_uuid": self.device_uuid,
            "timestamp": self.timestamp,
            "items": [asdict(item) for item in self.variables],
        })
