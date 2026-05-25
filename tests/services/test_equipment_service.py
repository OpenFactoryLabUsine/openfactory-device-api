


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

class TestEnrichUpdate:
    def test_returns_items_from_strategy(self, equipment_service, mock_ksql):
        mock_ksql.query.return_value = []
        msg = {"ID": "EQUIPMENT-1-var1", "VALUE": "5.0", "TIMESTAMP": "2024-01-01T00:00:00.000000"}
        items = equipment_service.enrich_update("EQUIPMENT-1", msg)
        assert len(items) >= 1
        assert items[0].id == "EQUIPMENT-1-var1"
        assert items[0].value == "5.0"