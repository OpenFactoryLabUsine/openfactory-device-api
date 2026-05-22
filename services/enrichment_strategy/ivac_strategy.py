from typing import Any

from models import Variable
from services.enrichment_strategy.device_enrichment_strategy import (
    DeviceEnrichmentStrategy,
)


class IvacStrategy(DeviceEnrichmentStrategy):
    def enrich_item(self, ksql_client, dataitem_id: str, value: Any, timestamp: str | None) -> list[Variable]:
        base = Variable(id=dataitem_id, value=value, kind="sample", timestamp=timestamp)
        try:
            result = ksql_client.query(
                f"SELECT IVAC_POWER_KEY, TOTAL_DURATION_SEC "
                f"FROM IVAC_POWER_STATE_TOTALS "
                f"WHERE IVAC_POWER_KEY LIKE '{dataitem_id}%';"
            )
            stats = [
                Variable(
                    id=f"stat:{_strip_prefix(row['IVAC_POWER_KEY'], dataitem_id)}",
                    value=row["TOTAL_DURATION_SEC"],
                    kind="stat",
                )
                for row in result
                if "IVAC_POWER_KEY" in row and "TOTAL_DURATION_SEC" in row
            ]
        except Exception as e:
            print(f"Error fetching IVAC stats for {dataitem_id}: {e}")
            stats = []
        return [base] + stats

def _strip_prefix(key: str, prefix: str) -> str:
    return key[len(prefix) + 1 :] if key.startswith(prefix + "_") else key
