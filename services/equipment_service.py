from openfactory.kafka import KSQLDBClient

from models import Variable
from services.enrichment_strategy.default_strategy import DefaultStrategy
from services.enrichment_strategy.dusttrak_strategy import DusttrakStrategy
from services.enrichment_strategy.equipment_enrichment_strategy import (
    DeviceEnrichmentStrategy,
)
from services.enrichment_strategy.ivac_strategy import IvacStrategy


class EquipmentService:
    _strategies: dict[str, DeviceEnrichmentStrategy] = {
        "IVAC": IvacStrategy(),
        "DUSTTRAK": DusttrakStrategy(),
    }
    _default_strategy = DefaultStrategy()

    def __init__(self, ksql_client: KSQLDBClient):
        self._ksql_client = ksql_client
        self._available_equipments = []

    def _get_strategy(self, asset_uuid: str) -> DeviceEnrichmentStrategy:
        for prefix, strategy in self._strategies.items():
            if asset_uuid.startswith(prefix):
                return strategy
        return self._default_strategy

    def get_all_equipments(self) -> list[str]:
        all_equipments = []
        self._available_equipments = []

        try:
            result = self._ksql_client.query(
                "SELECT ASSET_UUID FROM assets_type WHERE TYPE LIKE 'Device';"
            )
            all_equipments = [row["ASSET_UUID"] for row in result if row.get("ASSET_UUID")]

            for equipment in all_equipments:
                uppercase_equipment = equipment.upper()
                result = self._ksql_client.query(
                    f"SELECT AVAILABILITY FROM assets_avail WHERE ASSET_UUID = '{uppercase_equipment}';"
                )
                available = [row["AVAILABILITY"] for row in result if row.get("AVAILABILITY")]
                if available and available[0] == "AVAILABLE":
                    self._available_equipments.append(equipment)
        except Exception as e:
            print(f"Error getting equipments: {e}")
        return self._available_equipments

    
    def equipment_list_has_changed(self) -> bool:
        previous_state = self._available_equipments.copy()
        current_state = self.get_all_equipments().copy()

        return previous_state != current_state


    def get_equipment_variables(self, asset_uuid: str) -> dict:
        try:
            result = self._ksql_client.query(
                f"SELECT ID, VALUE FROM assets "
                f"WHERE ASSET_UUID = '{asset_uuid}' "
                f"AND TYPE IN ('Samples') "
                f"AND VALUE != 'UNAVAILABLE';"
            )
            return {
                row["ID"]: row["VALUE"]
                for row in result
                if "ID" in row and "VALUE" in row
            }
        except Exception as e:
            print(f"Error getting variables for {asset_uuid}: {e}")
            return {}

    def enrich_update(self, asset_uuid: str, msg_value: dict) -> list[Variable]:
        strategy = self._get_strategy(asset_uuid)
        return strategy.enrich_equipment_data(
            self._ksql_client,
            msg_value["ID"],
            msg_value.get("VALUE"),
            msg_value.get("TIMESTAMP"),
        )
