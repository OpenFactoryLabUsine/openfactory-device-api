import pytest

from services.enrichment_strategy.ivac_strategy import IvacStrategy


class TestIvacStrategy:
    @pytest.fixture
    def strategy(self):
        return IvacStrategy()

    def test_returns_base_item_plus_stats(self, strategy, mock_ksql):
        mock_ksql.query.return_value = [
            {"IVAC_POWER_KEY": "var1_on", "TOTAL_DURATION_SEC": 100},
            {"IVAC_POWER_KEY": "var1_off", "TOTAL_DURATION_SEC": 200},
        ]
        items = strategy.enrich_equipment_data(mock_ksql, "var1", "1.0", None)
        assert len(items) == 3
        assert items[0].kind == "sample"
        assert all(i.kind == "stat" for i in items[1:])

    def test_stat_ids_strip_prefix(self, strategy, mock_ksql):
        mock_ksql.query.return_value = [
            {"IVAC_POWER_KEY": "var1_on", "TOTAL_DURATION_SEC": 100},
        ]
        items = strategy.enrich_equipment_data(mock_ksql, "var1", "1.0", None)
        assert items[1].id == "stat:on"

    def test_returns_only_base_item_when_no_stats(self, strategy, mock_ksql):
        mock_ksql.query.return_value = []
        items = strategy.enrich_equipment_data(mock_ksql, "var1", "1.0", None)
        assert len(items) == 1
        assert items[0].kind == "sample"

    def test_returns_only_base_item_on_query_error(self, strategy, mock_ksql):
        mock_ksql.query.side_effect = Exception("ksql down")
        items = strategy.enrich_equipment_data(mock_ksql, "var1", "1.0", None)
        assert len(items) == 1
        assert items[0].kind == "sample"

    def test_skips_rows_missing_expected_keys(self, strategy, mock_ksql):
        mock_ksql.query.return_value = [
            {"IVAC_POWER_KEY": "var1_on"},
            {"TOTAL_DURATION_SEC": 200},
        ]
        items = strategy.enrich_equipment_data(mock_ksql, "var1", "1.0", None)
        assert len(items) == 1