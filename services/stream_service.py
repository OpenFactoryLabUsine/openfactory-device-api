from exceptions import StreamCreationException
from openfactory.kafka import KSQLDBClient


class StreamService:
    def __init__(self, ksql_client: KSQLDBClient):
        self._ksql_client = ksql_client

    def create_device_stream(self, device_uuid: str) -> str:
        topic_name = f"{device_uuid}_monitoring"
        try:
            self._ksql_client.statement_query(
                f"CREATE STREAM IF NOT EXISTS device_stream_{device_uuid} "
                f"WITH (KAFKA_TOPIC='{topic_name}', PARTITIONS=1) AS "
                f"SELECT ASSET_UUID AS KEY, ID, VALUE, "
                f"TIMESTAMPTOSTRING(ROWTIME, 'yyyy-MM-dd''T''HH:mm:ss[.nnnnnnn]', 'Canada/Eastern') AS TIMESTAMP "
                f"FROM ASSETS_STREAM WHERE ASSET_UUID = '{device_uuid}' "
                f"AND TYPE IN ('Events', 'Condition', 'Samples') AND VALUE != 'UNAVAILABLE' "
                f"EMIT CHANGES;"
            )
            return topic_name
        except Exception as e:
            raise StreamCreationException(
                f"Failed to create stream for device {device_uuid}: {e}"
            )

    def drop_device_stream(self, device_uuid: str):
        try:
            self._ksql_client.statement_query(
                f"DROP STREAM IF EXISTS device_stream_{device_uuid};"
            )
            print(f"Dropped stream for device {device_uuid}")
        except Exception as e:
            raise StreamCreationException(
                f"Failed to drop stream for device {device_uuid}: {e}"
            )
