from typing import Any

from openfactory.kafka import KSQLDBClient

from models import Variable
from services.enrichment_strategy.equipment_enrichment_strategy import (
    DeviceEnrichmentStrategy,
)


class DefaultStrategy(DeviceEnrichmentStrategy):
    def enrich_equipment_data(self, ksql_client: KSQLDBClient, variable_id: str, value: Any, timestamp: str | None) -> list[Variable]:
        return [Variable(id=variable_id, value=value, kind="sample", timestamp=timestamp)]




