from unittest.mock import MagicMock, patch

from models import DeviceDataItem


class TestDeviceMonitorIsActive:
    def test_returns_false_when_not_started(self, monitor):
        assert monitor.is_active("DEVICE-1") is False

    def test_returns_true_after_start(self, monitor, mock_openfactory_app):
        monitor._stream_service.create_device_stream = MagicMock(return_value="topic-1")
        monitor.start("DEVICE-1")
        assert monitor.is_active("DEVICE-1") is True


class TestDeviceMonitorStart:
    def test_initializes_asset_and_creates_stream(self, monitor, mock_openfactory_app):
        monitor._stream_service.create_device_stream = MagicMock(return_value="topic-1")
        monitor.start("DEVICE-1")
        mock_openfactory_app.initialize_asset.assert_called_once_with("DEVICE-1")
        monitor._stream_service.create_device_stream.assert_called_once_with("DEVICE-1")

    def test_does_not_start_twice(self, monitor):
        monitor._stream_service.create_device_stream = MagicMock(return_value="topic-1")
        monitor.start("DEVICE-1")
        monitor.start("DEVICE-1")
        assert monitor._stream_service.create_device_stream.call_count == 1

    def test_subscribes_to_topic(self, monitor):
        monitor._stream_service.create_device_stream = MagicMock(return_value="topic-1")
        monitor.start("DEVICE-1")
        monitor._topic_subscriber.subscribe.assert_called_once()
        call_kwargs = monitor._topic_subscriber.subscribe.call_args.kwargs
        assert call_kwargs["topic"] == "topic-1"


class TestDeviceMonitorStop:
    def test_drops_stream_on_stop(self, monitor):
        monitor._stream_service.create_device_stream = MagicMock(return_value="topic-1")
        monitor._stream_service.drop_device_stream = MagicMock()
        monitor.start("DEVICE-1")
        monitor.stop("DEVICE-1")
        monitor._stream_service.drop_device_stream.assert_called_once_with("DEVICE-1")

    def test_does_nothing_if_not_active(self, monitor):
        monitor._stream_service.drop_device_stream = MagicMock()
        monitor.stop("DEVICE-1")
        monitor._stream_service.drop_device_stream.assert_not_called()

    def test_stops_active_device(self, monitor):
        monitor._stream_service.create_device_stream = MagicMock(return_value="topic-1")
        monitor.start("DEVICE-1")
        monitor.stop("DEVICE-1")
        assert monitor.is_active("DEVICE-1") is False

    def test_unsubscribes_topic_on_stop(self, monitor):
        monitor._stream_service.create_device_stream = MagicMock(return_value="topic-1")
        monitor.start("DEVICE-1")
        monitor.stop("DEVICE-1")
        monitor._topic_subscriber.unsubscribe.assert_called_once_with("topic-1")


class TestDeviceMonitorOnMessage:
    def test_broadcasts_device_update_message(self, monitor):
        monitor._device_service.enrich_update = MagicMock(return_value=[
            DeviceDataItem(id="var1", value="1.0", kind="sample")
        ])
        with patch("monitoring.device_monitor.asyncio.get_event_loop") as mock_loop, \
             patch("monitoring.device_monitor.asyncio.run_coroutine_threadsafe") as mock_broadcast:
            mock_loop.return_value.is_running.return_value = True
            monitor._on_message("DEVICE-1", {"ID": "var1", "VALUE": "1.0"})
            mock_broadcast.assert_called_once()

    def test_does_not_raise_on_enrich_error(self, monitor):
        monitor._device_service.enrich_update = MagicMock(side_effect=Exception("fail"))
        monitor._on_message("DEVICE-1", {"ID": "var1", "VALUE": "1.0"})