from abc import ABC, abstractmethod
from typing import Any

from models import DeviceDataItem


class DeviceEnrichmentStrategy(ABC):
    @abstractmethod
    def enrich_item(self, ksql_client, dataitem_id: str, value: Any, timestamp: str | None) -> list[DeviceDataItem]:
        ...