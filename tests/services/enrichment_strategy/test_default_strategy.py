import pytest

from services.enrichment_strategy.default_strategy import DefaultStrategy


class TestDefaultStrategy:
    @pytest.fixture
    def strategy(self):
        return DefaultStrategy()

    def test_returns_single_sample_item(self, strategy, mock_ksql):
        items = strategy.enrich_equipment_data(mock_ksql, "var1", "1.0", None)
        assert len(items) == 1
        assert items[0].id == "var1"
        assert items[0].value == "1.0"
        assert items[0].kind == "sample"

    def test_passes_timestamp_through(self, strategy, mock_ksql):
        items = strategy.enrich_equipment_data(mock_ksql, "var1", "1.0", "2024-01-01T00:00:00.000000")
        assert items[0].timestamp == "2024-01-01T00:00:00.000000"

    def test_accepts_none_timestamp(self, strategy, mock_ksql):
        items = strategy.enrich_equipment_data(mock_ksql, "var1", "1.0", None)
        assert items[0].timestamp is None
