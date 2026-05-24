from unittest.mock import MagicMock, patch

from models import Variable


class TestEquipmentMonitorIsActive:
    def test_returns_false_when_not_started(self, monitor):
        assert monitor.is_active("EQUIPMENT-1") is False

    def test_returns_true_after_start(self, monitor, mock_openfactory_app):
        monitor._stream_service.create_equipment_stream = MagicMock(return_value="topic-1")
        monitor.start("EQUIPMENT-1")
        assert monitor.is_active("EQUIPMENT-1") is True


class TestEquipmentMonitorStart:
    def test_creates_stream(self, monitor, mock_openfactory_app):
        monitor._stream_service.create_equipment_stream = MagicMock(return_value="topic-1")
        monitor.start("EQUIPMENT-1")
        monitor._stream_service.create_equipment_stream.assert_called_once_with("EQUIPMENT-1")

    def test_does_not_start_twice(self, monitor):
        monitor._stream_service.create_equipment_stream = MagicMock(return_value="topic-1")
        monitor.start("EQUIPMENT-1")
        monitor.start("EQUIPMENT-1")
        assert monitor._stream_service.create_equipment_stream.call_count == 1

    def test_subscribes_to_topic(self, monitor):
        monitor._stream_service.create_equipment_stream = MagicMock(return_value="topic-1")
        monitor.start("EQUIPMENT-1")
        monitor._topic_subscriber.subscribe.assert_called_once()
        call_kwargs = monitor._topic_subscriber.subscribe.call_args.kwargs
        assert call_kwargs["topic"] == "topic-1"


class TestEquipmentMonitorStop:
    def test_drops_stream_on_stop(self, monitor):
        monitor._stream_service.create_equipment_stream = MagicMock(return_value="topic-1")
        monitor._stream_service.drop_equipment_stream = MagicMock()
        monitor.start("EQUIPMENT-1")
        monitor.stop("EQUIPMENT-1")
        monitor._stream_service.drop_equipment_stream.assert_called_once_with("EQUIPMENT-1")

    def test_does_nothing_if_not_active(self, monitor):
        monitor._stream_service.drop_equipment_stream = MagicMock()
        monitor.stop("EQUIPMENT-1")
        monitor._stream_service.drop_equipment_stream.assert_not_called()

    def test_stops_active_equipment(self, monitor):
        monitor._stream_service.create_equipment_stream = MagicMock(return_value="topic-1")
        monitor.start("EQUIPMENT-1")
        monitor.stop("EQUIPMENT-1")
        assert monitor.is_active("EQUIPMENT-1") is False

    def test_unsubscribes_topic_on_stop(self, monitor):
        monitor._stream_service.create_equipment_stream = MagicMock(return_value="topic-1")
        monitor.start("EQUIPMENT-1")
        monitor.stop("EQUIPMENT-1")
        monitor._topic_subscriber.unsubscribe.assert_called_once_with("topic-1")


class TestEquipmentMonitorOnMessage:
    def test_broadcasts_equipment_update_message(self, monitor):
        monitor._equipment_service.enrich_update = MagicMock(return_value=[
            Variable(id="var1", value="1.0", kind="sample")
        ])
        mock_loop = MagicMock()
        mock_loop.is_running.return_value = True
        monitor.set_event_loop(mock_loop)

        with patch("monitoring.equipment_monitor.asyncio.run_coroutine_threadsafe") as mock_broadcast:
            monitor._on_message("EQUIPMENT-1", {"ID": "var1", "VALUE": "1.0"})
            mock_broadcast.assert_called_once()

    def test_does_not_raise_on_enrich_error(self, monitor):
        monitor._equipment_service.enrich_update = MagicMock(side_effect=Exception("fail"))
        monitor._on_message("EQUIPMENT-1", {"ID": "var1", "VALUE": "1.0"})