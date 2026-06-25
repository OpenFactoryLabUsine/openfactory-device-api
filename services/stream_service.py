from openfactory.kafka import KSQLDBClient

from exceptions import StreamCreationException


class StreamService:
    def __init__(self, ksql_client: KSQLDBClient):
        self._ksql_client = ksql_client

    def create_equipment_stream(self, asset_uuid: str) -> str:
        parsed_uuid = asset_uuid.replace("-", "_")
        topic_name = f"{parsed_uuid}_MONITORING".upper()
        print(f"Creating stream EQUIPMENT_STREAM_{parsed_uuid} on topic {topic_name}")
        try:
            self._ksql_client.statement_query(
            f"DROP STREAM IF EXISTS EQUIPMENT_STREAM_{parsed_uuid};"
            )   

            self._ksql_client.statement_query(
                f"CREATE STREAM EQUIPMENT_STREAM_{parsed_uuid} "
                f"WITH (KAFKA_TOPIC='{topic_name}', PARTITIONS=1) AS "
                f"SELECT '{asset_uuid}' AS ASSET_UUID, ID, VALUE, TIMESTAMP "
                f"FROM ENRICHED_ASSETS_STREAM "
                f"WHERE ASSET_UUID = '{asset_uuid}' "
                f"AND TYPE IN ('Events', 'Condition', 'Samples') AND VALUE != 'UNAVAILABLE' "
                f"PARTITION BY '{asset_uuid}' "
                f"EMIT CHANGES;"
            )
            return topic_name
        except Exception as e:
            raise StreamCreationException(
                f"Failed to create stream for equipment {asset_uuid}: {e}"
            ) from e

    def drop_equipment_stream(self, asset_uuid: str):
        try:
            self._ksql_client.statement_query(
                f"DROP STREAM IF EXISTS EQUIPMENT_STREAM_{asset_uuid};"
            )
            print(f"Dropped stream for equipment {asset_uuid}")
        except Exception as e:
            raise StreamCreationException(
                f"Failed to drop stream for equipment {asset_uuid}: {e}"
            ) from e
