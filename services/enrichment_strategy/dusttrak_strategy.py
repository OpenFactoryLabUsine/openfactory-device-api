from datetime import datetime, timedelta, timezone
from typing import Any

from models import Variable
from services.enrichment_strategy.equipment_enrichment_strategy import (
    DeviceEnrichmentStrategy,
)


class DusttrakStrategy(DeviceEnrichmentStrategy):
    _TABLE_NOT_FOUND_CODE = 40001
    _DB_TIMEZONE_OFFSET = timedelta(hours=-4)
    _unavailable_tables: set[str] = set()

    def _convert_to_db_timezone(self, timestamp: str) -> str:
        dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        dt_local = dt.astimezone(timezone(self._DB_TIMEZONE_OFFSET))
        return dt_local.strftime("%Y-%m-%dT%H:%M:%S")

    def enrich_equipment_data(self, ksql_client, variable_id: str, value: Any, timestamp: str | None) -> list[Variable]:
        base = Variable(id=variable_id, value=value, kind="sample", timestamp=timestamp)
        
        table_name = f"{variable_id}_moving_average".upper()
        if table_name in self._unavailable_tables:
            return [base]

        try:
            db_timestamp = self._convert_to_db_timezone(timestamp)
            timestamp_prefix = db_timestamp[:-4]
            result = ksql_client.query(
                f"SELECT AVERAGE_VALUE, TIMESTAMP "
                f"FROM {table_name} "
                f"WHERE timestamp LIKE '{timestamp_prefix}%';"
            )
            first_row = next(
                (r for r in result if "AVERAGE_VALUE" in r and "TIMESTAMP" in r), None
            )
            print(f"Table queried: {table_name}, query: SELECT AVERAGE_VALUE, TIMESTAMP FROM {table_name} WHERE timestamp LIKE '{timestamp_prefix}%';")
            if first_row:
                avg = Variable(
                    id=f"avg:{variable_id}",
                    value=first_row["AVERAGE_VALUE"],
                    kind="avg",
                    timestamp=first_row["TIMESTAMP"],
                )
                print(f"Fetched average for {variable_id} at {timestamp}: {avg.value}")
                return [base, avg]
            
            print(f"No valid average found for {variable_id} at {timestamp}.")

        except Exception as e:
            error_str = str(e)
            if str(self._TABLE_NOT_FOUND_CODE) in error_str and "does not exist" in error_str:
                print(f"Moving average table '{table_name}' not available, skipping until restart.")
                self._unavailable_tables.add(table_name)
            else:
                print(f"Error fetching avg for {variable_id}: {e}")

        return [base]
