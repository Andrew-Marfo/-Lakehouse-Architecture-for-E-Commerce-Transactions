# tests/unit/test_product_etl.py
from unittest.mock import patch
from src.glue_scripts.product_etl import transform_product_data

def test_transform_product_data_deduplicates(spark, sample_product_data):
    with patch("delta.tables.DeltaTable.isDeltaTable") as mock_delta:
        mock_delta.return_value = False
        result = transform_product_data(sample_product_data, "dummy_path")
        assert result.count() == 2  # Deduplicated count