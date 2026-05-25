
from models import Variable


class TestGetAllDevices:
    def test_returns_all_equipments(self, equipment_service, mock_ksql):
        mock_ksql.query.side_effect = [
            [{"ASSET_UUID": "EQUIPMENT-1"}, {"ASSET_UUID": "EQUIPMENT-2"}],
            [{"AVAILABILITY": "AVAILABLE"}],
            [{"AVAILABILITY": "AVAILABLE"}]
        ]
        
        assert equipment_service.get_all_equipments() == ["EQUIPMENT-1", "EQUIPMENT-2"]

    def test_skips_rows_missing_asset_uuid(self, equipment_service, mock_ksql):
        mock_ksql.query.side_effect = [
            [{"ASSET_UUID": "EQUIPMENT-1"}, {}],
            [{"AVAILABILITY": "AVAILABLE"}]
        ]
        
        assert equipment_service.get_all_equipments() == ["EQUIPMENT-1"]

    def test_skips_unavailable_equipments(self, equipment_service, mock_ksql):
        mock_ksql.query.side_effect = [
            [{"ASSET_UUID": "EQUIPMENT-1"}, {"ASSET_UUID": "EQUIPMENT-2"}],
            [{"AVAILABILITY": "AVAILABLE"}],
            [{"AVAILABILITY": "UNAVAILABLE"}]
        ]
        
        assert equipment_service.get_all_equipments() == ["EQUIPMENT-1"]

    def test_returns_empty_list_on_error(self, equipment_service, mock_ksql):
        mock_ksql.query.side_effect = Exception("ksql down")
        assert equipment_service.get_all_equipments() == []


class TestGetInitialItems:
    def test_returns_items_for_each_dataitem(self, equipment_service, mock_ksql):
        mock_ksql.query.return_value = [
            {"ID": "EQUIPMENT-1-var1", "VALUE": "1.0"},
            {"ID": "EQUIPMENT-1-var2", "VALUE": "2.0"},
        ]
        items = equipment_service.get_initial_variables("EQUIPMENT-1")
        assert len(items) == 2
        assert all(isinstance(i, Variable) for i in items)
        assert items[0].id == "EQUIPMENT-1-var1"
        assert items[1].id == "EQUIPMENT-1-var2"

    def test_skips_rows_missing_id_or_value(self, equipment_service, mock_ksql):
        mock_ksql.query.return_value = [
            {"ID": "EQUIPMENT-1-var1", "VALUE": "1.0"},
            {"ID": "EQUIPMENT-1-var2"},
            {"VALUE": "3.0"},
        ]
        items = equipment_service.get_initial_variables("EQUIPMENT-1")
        assert len(items) == 1
        assert items[0].id == "EQUIPMENT-1-var1"

    def test_returns_empty_list_on_error(self, equipment_service, mock_ksql):
        mock_ksql.query.side_effect = Exception("ksql down")
        assert equipment_service.get_initial_variables("EQUIPMENT-1") == []

    def test_uses_ivac_strategy_for_ivac_equipment(self, equipment_service, mock_ksql):
        mock_ksql.query.side_effect = [
            [{"ID": "IVAC-1-var1", "VALUE": "1.0"}],
            [{"IVAC_POWER_KEY": "IVAC-1-var1_on", "TOTAL_DURATION_SEC": 100}],
        ]
        items = equipment_service.get_initial_variables("IVAC-1")
        kinds = {i.kind for i in items}
        assert "stat" in kinds

    def test_uses_default_strategy_for_unknown_equipment(self, equipment_service, mock_ksql):
        mock_ksql.query.return_value = [{"ID": "UNKNOWN-1-var1", "VALUE": "42.0"}]
        items = equipment_service.get_initial_variables("UNKNOWN-1")
        assert len(items) == 1
        assert items[0].kind == "sample"


class TestEnrichUpdate:
    def test_returns_items_from_strategy(self, equipment_service, mock_ksql):
        mock_ksql.query.return_value = []
        msg = {"ID": "EQUIPMENT-1-var1", "VALUE": "5.0", "TIMESTAMP": "2024-01-01T00:00:00.000000"}
        items = equipment_service.enrich_update("EQUIPMENT-1", msg)
        assert len(items) >= 1
        assert items[0].id == "EQUIPMENT-1-var1"
        assert items[0].value == "5.0"