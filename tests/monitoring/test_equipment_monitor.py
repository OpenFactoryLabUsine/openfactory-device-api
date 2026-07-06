import asyncio
from unittest.mock import MagicMock, patch

import pytest

from models import Variable


class TestEquipmentMonitorIsActive:
    def test_returns_false_when_not_started(self, monitor):
        assert monitor.is_active("EQUIPMENT-1") is False

    def test_returns_true_after_start(self, monitor):
        monitor.start("EQUIPMENT-1")
        assert monitor.is_active("EQUIPMENT-1") is True


class TestEquipmentMonitorStart:
    def test_subscribes_to_the_uppercased_subject(self, monitor):
        monitor.start("EQUIPMENT-1")
        monitor._asset_subscriber.subscribe.assert_called_once()
        call_kwargs = monitor._asset_subscriber.subscribe.call_args.kwargs
        assert call_kwargs["subject"] == "EQUIPMENT-1.>"
        assert call_kwargs["message_filter"]("EQUIPMENT-1") is True
        assert call_kwargs["message_filter"]("OTHER") is False

    def test_does_not_duplicate_an_existing_monitor(self, monitor):
        monitor.start("EQUIPMENT-1")
        monitor.start("EQUIPMENT-1")
        assert monitor._asset_subscriber.subscribe.call_count == 1

    def test_raises_when_asset_initialization_fails(self, monitor):
        monitor._initialize_asset = MagicMock(side_effect=RuntimeError("boom"))

        with pytest.raises(RuntimeError):
            monitor.start("EQUIPMENT-1")


class TestEquipmentMonitorStop:
    def test_stops_active_equipment(self, monitor):
        monitor.start("EQUIPMENT-1")
        monitor.stop("EQUIPMENT-1")
        assert monitor.is_active("EQUIPMENT-1") is False

    def test_unsubscribes_the_monitored_subject_on_stop(self, monitor):
        monitor.start("EQUIPMENT-1")
        monitor.stop("EQUIPMENT-1")
        monitor._asset_subscriber.unsubscribe.assert_called_once_with("EQUIPMENT-1.>")

    def test_does_nothing_for_unknown_equipment(self, monitor):
        monitor.stop("EQUIPMENT-1")
        monitor._asset_subscriber.unsubscribe.assert_not_called()


class TestEquipmentMonitorOnMessage:
    def test_broadcasts_equipment_update_message_when_loop_is_running(self, monitor):
        variables = [Variable(id="var1", value="1.0", kind="sample")]
        monitor._equipment_service.enrich_update = MagicMock(return_value=variables)
        mock_loop = MagicMock()
        mock_loop.is_running.return_value = True
        monitor.set_event_loop(mock_loop)

        with patch("monitoring.equipment_monitor.asyncio.run_coroutine_threadsafe") as mock_broadcast:
            monitor._on_message("EQUIPMENT-1", {"ID": "var1", "VALUE": "1.0"})

        mock_broadcast.assert_called_once()
        args = mock_broadcast.call_args.args
        assert len(args) == 2
        assert asyncio.iscoroutine(args[0])
        assert args[1] is mock_loop

    def test_drops_messages_when_no_event_loop_is_available(self, monitor):
        monitor._equipment_service.enrich_update = MagicMock(return_value=[])
        monitor.set_event_loop(None)

        with patch("monitoring.equipment_monitor.asyncio.get_event_loop", return_value=MagicMock(is_running=MagicMock(return_value=False))):
            monitor._on_message("EQUIPMENT-1", {"ID": "var1", "VALUE": "1.0"})

    def test_does_not_raise_on_enrich_error(self, monitor):
        monitor._equipment_service.enrich_update = MagicMock(side_effect=Exception("fail"))
        monitor._on_message("EQUIPMENT-1", {"ID": "var1", "VALUE": "1.0"})