from services.strategy.device_enrichment_strategy.device_enrichment_strategy import DeviceEnrichmentStrategy
from openfactory.kafka import KSQLDBClient

class DefaultStrategy(DeviceEnrichmentStrategy):
    def get_stats(self, ksql_client: KSQLDBClient, device_uuid: str) -> dict:
        return {}

    def process_update(self, ksql_client: KSQLDBClient, msg_value: dict):
        pass



