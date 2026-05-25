import pytest

from exceptions import StreamCreationException


class TestCreateDeviceStream:
    def test_returns_topic_name(self, stream_service, mock_ksql):
        topic = stream_service.create_equipment_stream("EQUIPMENT-1")
        assert topic == "EQUIPMENT-1_MONITORING"

    def test_executes_create_stream_statement(self, stream_service, mock_ksql):
        stream_service.create_equipment_stream("EQUIPMENT-1")
        mock_ksql.statement_query.assert_called_once()
        query = mock_ksql.statement_query.call_args.args[0]
        assert "EQUIPMENT_STREAM_EQUIPMENT-1" in query
        assert "EQUIPMENT-1_MONITORING" in query

    def test_raises_stream_creation_exception_on_error(self, stream_service, mock_ksql):
        mock_ksql.statement_query.side_effect = Exception("ksql down")
        with pytest.raises(StreamCreationException):
            stream_service.create_equipment_stream("EQUIPMENT-1")


class TestDropDeviceStream:
    def test_executes_drop_connection_statement(self, stream_service, mock_ksql):
        stream_service.drop_equipment_stream("EQUIPMENT-1")
        mock_ksql.statement_query.assert_called_once()
        query = mock_ksql.statement_query.call_args.args[0]
        assert "EQUIPMENT_STREAM_EQUIPMENT-1" in query

    def test_raises_stream_creation_exception_on_error(self, stream_service, mock_ksql):
        mock_ksql.statement_query.side_effect = Exception("ksql down")
        with pytest.raises(StreamCreationException):
            stream_service.drop_equipment_stream("EQUIPMENT-1")