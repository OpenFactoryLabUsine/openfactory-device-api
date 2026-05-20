from abc import ABC, abstractmethod
from openfactory.kafka import KSQLDBClient


class DeviceEnrichmentStrategy(ABC):
    @abstractmethod
    def get_stats(self, ksql_client: KSQLDBClient, device_uuid: str) -> dict:
        pass

    @abstractmethod
    def process_update(self, ksql_client: KSQLDBClient, msg_value: dict):
        pass
