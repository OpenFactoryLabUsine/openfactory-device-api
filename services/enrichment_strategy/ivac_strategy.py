from typing import Any

from models import Variable
from services.enrichment_strategy.equipment_enrichment_strategy import (
    DeviceEnrichmentStrategy,
)

_TABLE_NAME = "IVAC_POWER_STATE_TOTALS"
_TABLE_NOT_FOUND_CODE = "40001"


class IvacStrategy(DeviceEnrichmentStrategy):
    _unavailable_until: float = 0
    _RETRY_AFTER_SECONDS = 60

    def enrich_equipment_data(self, ksql_client, variable_id: str, value: Any, timestamp: str | None) -> list[Variable]:
        from time import monotonic
        base = Variable(id=variable_id, value=value, kind="sample", timestamp=timestamp)

        if monotonic() < self._unavailable_until:
            return [base]

        try:
            result = ksql_client.query(
                f"SELECT IVAC_POWER_KEY, TOTAL_DURATION_SEC "
                f"FROM {_TABLE_NAME} "
                f"WHERE IVAC_POWER_KEY LIKE '{variable_id}%';"
            )
            stats = [
                Variable(
                    id=f"stat:{_strip_prefix(row['IVAC_POWER_KEY'], variable_id)}",
                    value=row["TOTAL_DURATION_SEC"],
                    kind="stat",
                )
                for row in result
                if "IVAC_POWER_KEY" in row and "TOTAL_DURATION_SEC" in row
            ]
        except Exception as e:
            error_str = str(e)
            if _TABLE_NOT_FOUND_CODE in error_str and "does not exist" in error_str:
                print(f"Table '{_TABLE_NAME}' not available, retrying in {self._RETRY_AFTER_SECONDS}s.")
                self._unavailable_until = monotonic() + self._RETRY_AFTER_SECONDS
            else:
                print(f"Error fetching IVAC stats for {variable_id}: {e}")
            stats = []

        return [base] + stats


def _strip_prefix(key: str, prefix: str) -> str:
    return key[len(prefix) + 1:] if key.startswith(prefix + "_") else key