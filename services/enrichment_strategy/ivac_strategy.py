from services.strategy.device_enrichment_strategy.device_enrichment_strategy import DeviceEnrichmentStrategy
from openfactory.kafka import KSQLDBClient

class IvacStrategy(DeviceEnrichmentStrategy):
    def get_stats(self, ksql_client: KSQLDBClient, device_uuid: str) -> dict:
        try:
            result = ksql_client.query(
                f"SELECT IVAC_POWER_KEY, TOTAL_DURATION_SEC "
                f"FROM IVAC_POWER_STATE_TOTALS "
                f"WHERE IVAC_POWER_KEY LIKE '{device_uuid}%';"
            )
            return {
                _strip_prefix(row["IVAC_POWER_KEY"], device_uuid): row[
                    "TOTAL_DURATION_SEC"
                ]
                for row in result
                if "IVAC_POWER_KEY" in row and "TOTAL_DURATION_SEC" in row
            }
        except Exception as e:
            print(f"Error getting IVAC stats for {device_uuid}: {e}")
            return {}

    def process_update(self, ksql_client: KSQLDBClient, msg_value: dict):
        dataitem_id = msg_value.get("ID")
        if not dataitem_id:
            return
        try:
            result = ksql_client.query(
                f"SELECT IVAC_POWER_KEY, TOTAL_DURATION_SEC "
                f"FROM IVAC_POWER_STATE_TOTALS "
                f"WHERE IVAC_POWER_KEY LIKE '{dataitem_id}%';"
            )
            msg_value["durations"] = {
                _strip_prefix(row["IVAC_POWER_KEY"], dataitem_id): row[
                    "TOTAL_DURATION_SEC"
                ]
                for row in result
                if "IVAC_POWER_KEY" in row and "TOTAL_DURATION_SEC" in row
            }
        except Exception as e:
            print(f"Error adding duration updates for {dataitem_id}: {e}")
            msg_value["durations"] = {}

def _strip_prefix(key: str, prefix: str) -> str:
    return key[len(prefix) + 1 :] if key.startswith(prefix + "_") else key
