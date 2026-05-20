from services.strategy.device_enrichment_strategy.device_enrichment_strategy import DeviceEnrichmentStrategy
from openfactory.kafka import KSQLDBClient

class DusttrakStrategy(DeviceEnrichmentStrategy):
    def get_stats(self, ksql_client: KSQLDBClient, device_uuid: str) -> dict:
        return {}

    def process_update(self, ksql_client: KSQLDBClient, msg_value: dict):
        dataitem_id = msg_value.get("ID")
        timestamp = msg_value.get("TIMESTAMP")
        if not dataitem_id or not timestamp:
            return
        try:
            result = ksql_client.query(
                f"SELECT AVERAGE_VALUE, TIMESTAMP "
                f"FROM {dataitem_id}_moving_average "
                f"WHERE timestamp LIKE '{timestamp[:-10]}%';"
            )
            first_row = next(
                (r for r in result if "AVERAGE_VALUE" in r and "TIMESTAMP" in r),
                None,
            )
            msg_value["avg_value"] = (
                {
                    "value": first_row["AVERAGE_VALUE"],
                    "timestamp": first_row["TIMESTAMP"],
                }
                if first_row
                else {}
            )
        except Exception as e:
            print(f"Error adding avg values for {dataitem_id}: {e}")
            msg_value["avg_value"] = {}
