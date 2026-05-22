from abc import ABC, abstractmethod
from typing import Any

from models import Variable


class DeviceEnrichmentStrategy(ABC):
    @abstractmethod
    def enrich_item(self, ksql_client, dataitem_id: str, value: Any, timestamp: str | None) -> list[Variable]:
        ...