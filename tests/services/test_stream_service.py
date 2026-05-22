import pytest

from exceptions import StreamCreationException


class TestCreateDeviceStream:
    def test_returns_topic_name(self, stream_service, mock_ksql):
        topic = stream_service.create_device_stream("DEVICE-1")
        assert topic == "DEVICE-1_monitoring"

    def test_executes_create_stream_statement(self, stream_service, mock_ksql):
        stream_service.create_device_stream("DEVICE-1")
        mock_ksql.statement_query.assert_called_once()
        query = mock_ksql.statement_query.call_args.args[0]
        assert "device_stream_DEVICE-1" in query
        assert "DEVICE-1_monitoring" in query

    def test_raises_stream_creation_exception_on_error(self, stream_service, mock_ksql):
        mock_ksql.statement_query.side_effect = Exception("ksql down")
        with pytest.raises(StreamCreationException):
            stream_service.create_device_stream("DEVICE-1")


class TestDropDeviceStream:
    def test_executes_drop_stream_statement(self, stream_service, mock_ksql):
        stream_service.drop_device_stream("DEVICE-1")
        mock_ksql.statement_query.assert_called_once()
        query = mock_ksql.statement_query.call_args.args[0]
        assert "device_stream_DEVICE-1" in query

    def test_raises_stream_creation_exception_on_error(self, stream_service, mock_ksql):
        mock_ksql.statement_query.side_effect = Exception("ksql down")
        with pytest.raises(StreamCreationException):
            stream_service.drop_device_stream("DEVICE-1")