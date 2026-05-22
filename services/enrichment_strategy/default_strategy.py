from typing import Any

from openfactory.kafka import KSQLDBClient

from models import DeviceDataItem
from services.enrichment_strategy.device_enrichment_strategy import (
    DeviceEnrichmentStrategy,
)


class DefaultStrategy(DeviceEnrichmentStrategy):
    def enrich_item(self, ksql_client: KSQLDBClient, dataitem_id: str, value: Any, timestamp: str | None) -> list[DeviceDataItem]:
        return [DeviceDataItem(id=dataitem_id, value=value, kind="sample", timestamp=timestamp)]




