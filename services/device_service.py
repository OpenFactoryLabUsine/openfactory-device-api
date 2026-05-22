from openfactory.kafka import KSQLDBClient

from models import Variable
from services.enrichment_strategy.default_strategy import DefaultStrategy
from services.enrichment_strategy.device_enrichment_strategy import (
    DeviceEnrichmentStrategy,
)
from services.enrichment_strategy.dusttrak_strategy import DusttrakStrategy
from services.enrichment_strategy.ivac_strategy import IvacStrategy


class DeviceService:
    _strategies: dict[str, DeviceEnrichmentStrategy] = {
        "IVAC": IvacStrategy(),
        "DUSTTRAK": DusttrakStrategy(),
    }
    _default_strategy = DefaultStrategy()

    def __init__(self, ksql_client: KSQLDBClient):
        self._ksql_client = ksql_client

    def _get_strategy(self, device_uuid: str) -> DeviceEnrichmentStrategy:
        for prefix, strategy in self._strategies.items():
            if device_uuid.startswith(prefix):
                return strategy
        return self._default_strategy

    def get_all_devices(self) -> list[str]:
        try:
            result = self._ksql_client.query(
                "SELECT ASSET_UUID FROM assets_type WHERE TYPE LIKE 'Device';"
            )
            return [row["ASSET_UUID"] for row in result if row.get("ASSET_UUID")]
        except Exception as e:
            print(f"Error getting devices: {e}")
            return []

    def get_device_variables(self, device_uuid: str) -> dict:
        try:
            result = self._ksql_client.query(
                f"SELECT ID, VALUE FROM assets "
                f"WHERE ASSET_UUID = '{device_uuid}' "
                f"AND TYPE IN ('Samples') "
                f"AND VALUE != 'UNAVAILABLE';"
            )
            return {
                row["ID"]: row["VALUE"]
                for row in result
                if "ID" in row and "VALUE" in row
            }
        except Exception as e:
            print(f"Error getting dataitems for {device_uuid}: {e}")
            return {}

    def get_initial_variables(self, device_uuid: str) -> list[Variable]:
        try:
            result = self._ksql_client.query(
                f"SELECT ID, VALUE FROM assets "
                f"WHERE ASSET_UUID = '{device_uuid}' "
                f"AND TYPE IN ('Samples') "
                f"AND VALUE != 'UNAVAILABLE';"
            )
            strategy = self._get_strategy(device_uuid)
            items = []
            for row in result:
                if "ID" in row and "VALUE" in row:
                    items.extend(strategy.enrich_item(self._ksql_client, row["ID"], row["VALUE"], None))
            return items
        except Exception as e:
            print(f"Error getting initial items for {device_uuid}: {e}")
            return []

    def enrich_update(self, device_uuid: str, msg_value: dict) -> list[Variable]:
        strategy = self._get_strategy(device_uuid)
        return strategy.enrich_item(
            self._ksql_client,
            msg_value["ID"],
            msg_value.get("VALUE"),
            msg_value.get("TIMESTAMP"),
        )
