import pytest

from services.enrichment_strategy.dusttrak_strategy import DusttrakStrategy


class TestDusttrakStrategy:
    @pytest.fixture
    def strategy(self):
        return DusttrakStrategy()

    def test_returns_base_plus_avg_when_found(self, strategy, mock_ksql):
        mock_ksql.query.return_value = [
            {"AVERAGE_VALUE": "3.5", "TIMESTAMP": "2024-01-01T00:00:00.000000"},
        ]
        items = strategy.enrich_equipment_data(mock_ksql, "var1", "1.0", "2024-01-01T00:00:10.000000")
        assert len(items) == 2
        assert items[0].kind == "sample"
        assert items[1].kind == "avg"
        assert items[1].id == "avg:var1"
        assert items[1].value == "3.5"

    def test_returns_only_base_when_no_avg_found(self, strategy, mock_ksql):
        mock_ksql.query.return_value = []
        items = strategy.enrich_equipment_data(mock_ksql, "var1", "1.0", "2024-01-01T00:00:10.000000")
        assert len(items) == 1
        assert items[0].kind == "sample"

    def test_returns_only_base_on_none_timestamp(self, strategy, mock_ksql):
        items = strategy.enrich_equipment_data(mock_ksql, "var1", "1.0", None)
        assert len(items) == 1
        assert items[0].kind == "sample"

    def test_returns_only_base_on_query_error(self, strategy, mock_ksql):
        mock_ksql.query.side_effect = Exception("ksql down")
        items = strategy.enrich_equipment_data(mock_ksql, "var1", "1.0", "2024-01-01T00:00:10.000000")
        assert len(items) == 1

    def test_skips_avg_row_missing_expected_keys(self, strategy, mock_ksql):
        mock_ksql.query.return_value = [{"AVERAGE_VALUE": "3.5"}]
        items = strategy.enrich_equipment_data(mock_ksql, "var1", "1.0", "2024-01-01T00:00:10.000000")
        assert len(items) == 1