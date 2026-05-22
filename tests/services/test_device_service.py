
from models import DeviceDataItem


class TestGetAllDevices:
    def test_returns_all_devices(self, device_service, mock_ksql):
        mock_ksql.query.return_value = [{"ASSET_UUID": "DEVICE-1"}, {"ASSET_UUID": "DEVICE-2"}]
        assert device_service.get_all_devices() == ["DEVICE-1", "DEVICE-2"]

    def test_skips_rows_missing_asset_uuid(self, device_service, mock_ksql):
        mock_ksql.query.return_value = [{"ASSET_UUID": "DEVICE-1"}, {}]
        assert device_service.get_all_devices() == ["DEVICE-1"]

    def test_returns_empty_list_on_error(self, device_service, mock_ksql):
        mock_ksql.query.side_effect = Exception("ksql down")
        assert device_service.get_all_devices() == []


class TestGetInitialItems:
    def test_returns_items_for_each_dataitem(self, device_service, mock_ksql):
        mock_ksql.query.return_value = [
            {"ID": "DEVICE-1-var1", "VALUE": "1.0"},
            {"ID": "DEVICE-1-var2", "VALUE": "2.0"},
        ]
        items = device_service.get_initial_items("DEVICE-1")
        assert len(items) == 2
        assert all(isinstance(i, DeviceDataItem) for i in items)
        assert items[0].id == "DEVICE-1-var1"
        assert items[1].id == "DEVICE-1-var2"

    def test_skips_rows_missing_id_or_value(self, device_service, mock_ksql):
        mock_ksql.query.return_value = [
            {"ID": "DEVICE-1-var1", "VALUE": "1.0"},
            {"ID": "DEVICE-1-var2"},
            {"VALUE": "3.0"},
        ]
        items = device_service.get_initial_items("DEVICE-1")
        assert len(items) == 1
        assert items[0].id == "DEVICE-1-var1"

    def test_returns_empty_list_on_error(self, device_service, mock_ksql):
        mock_ksql.query.side_effect = Exception("ksql down")
        assert device_service.get_initial_items("DEVICE-1") == []

    def test_uses_ivac_strategy_for_ivac_device(self, device_service, mock_ksql):
        mock_ksql.query.side_effect = [
            [{"ID": "IVAC-1-var1", "VALUE": "1.0"}],
            [{"IVAC_POWER_KEY": "IVAC-1-var1_on", "TOTAL_DURATION_SEC": 100}],
        ]
        items = device_service.get_initial_items("IVAC-1")
        kinds = {i.kind for i in items}
        assert "stat" in kinds

    def test_uses_default_strategy_for_unknown_device(self, device_service, mock_ksql):
        mock_ksql.query.return_value = [{"ID": "UNKNOWN-1-var1", "VALUE": "42.0"}]
        items = device_service.get_initial_items("UNKNOWN-1")
        assert len(items) == 1
        assert items[0].kind == "sample"


class TestEnrichUpdate:
    def test_returns_items_from_strategy(self, device_service, mock_ksql):
        mock_ksql.query.return_value = []
        msg = {"ID": "DEVICE-1-var1", "VALUE": "5.0", "TIMESTAMP": "2024-01-01T00:00:00.000000"}
        items = device_service.enrich_update("DEVICE-1", msg)
        assert len(items) >= 1
        assert items[0].id == "DEVICE-1-var1"
        assert items[0].value == "5.0"