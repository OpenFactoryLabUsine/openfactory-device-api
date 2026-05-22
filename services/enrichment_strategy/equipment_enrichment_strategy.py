from abc import ABC, abstractmethod
from typing import Any

from models import Variable


class DeviceEnrichmentStrategy(ABC):
    @abstractmethod
    def enrich_equipment_data(self, ksql_client, variable_id: str, value: Any, timestamp: str | None) -> list[Variable]:
        ...