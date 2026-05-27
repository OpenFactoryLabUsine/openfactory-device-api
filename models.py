from dataclasses import dataclass
from typing import Any


@dataclass
class ClientMessage:
    method: str
    params: dict[str, Any]

    @classmethod
    def from_dict(cls, data: dict) -> "ClientMessage":
        return cls(
            method=data.get("method", ""),
            params=data.get("params", {}),
        )

@dataclass
class Variable:
    id: str
    value: Any
    kind: str = "sample"
    timestamp: str | None = None