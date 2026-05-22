from typing import Any

from models import Variable
from services.enrichment_strategy.device_enrichment_strategy import (
    DeviceEnrichmentStrategy,
)


class DusttrakStrategy(DeviceEnrichmentStrategy):
    def enrich_item(self, ksql_client, dataitem_id: str, value: Any, timestamp: str | None) -> list[Variable]:
        base = Variable(id=dataitem_id, value=value, kind="sample", timestamp=timestamp)
        try:
            result = ksql_client.query(
                f"SELECT AVERAGE_VALUE, TIMESTAMP "
                f"FROM {id}_moving_average "
                f"WHERE timestamp LIKE '{timestamp[:-10]}%';"
            )
            first_row = next(
                (r for r in result if "AVERAGE_VALUE" in r and "TIMESTAMP" in r), None
            )
            if first_row:
                avg = Variable(
                    id=f"avg:{dataitem_id}",
                    value=first_row["AVERAGE_VALUE"],
                    kind="avg",
                    timestamp=first_row["TIMESTAMP"],
                )
                return [base, avg]
        except Exception as e:
            print(f"Error fetching avg for {id}: {e}")
        return [base]
