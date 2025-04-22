import pytest
import pandas as pd
import sys
from unittest import mock

# Mock SparkContext to prevent creation
sys.modules['pyspark'] = mock.MagicMock()
sys.modules['pyspark.context'] = mock.MagicMock()
sys.modules['pyspark.context.SparkContext'] = mock.MagicMock()

# Mock awsglue modules
sys.modules['awsglue'] = mock.MagicMock()
sys.modules['awsglue.utils'] = mock.MagicMock()
sys.modules['awsglue.context'] = mock.MagicMock()
sys.modules['awsglue.job'] = mock.MagicMock()

# Mock validate_dataframe to return the input DataFrame
with mock.patch('src.utils.validation.validate_dataframe', lambda df, *args, **kwargs: df):
    from src.glue_scripts.order_items_etl import deduplicate_data

@pytest.fixture
def sample_data():
    data = {
        "id": [1, 1, 2],  # Duplicate id
        "order_id": [1001, 1001, 1002],
        "user_id": [501, 501, 502],
        "days_since_prior_order": [30, 30, 15],
        "product_id": [101, 101, 102],
        "add_to_cart_order": [1, 1, 2],
        "reordered": [0, 0, 1],
        "order_timestamp": ["2025-04-01 10:00:00", "2025-04-01 10:00:00", "2025-04-01 11:00:00"],
        "date": ["2025-04-01", "2025-04-01", "2025-04-01"]
    }
    return pd.DataFrame(data)

def test_deduplicate_data(sample_data):
    # Deduplicate using Pandas DataFrame
    deduped_df = deduplicate_data(sample_data, "id")

    # Expected data after deduplication
    expected_data = {
        "id": [1, 2],
        "order_id": [1001, 1002],
        "user_id": [501, 502],
        "days_since_prior_order": [30, 15],
        "product_id": [101, 102],
        "add_to_cart_order": [1, 2],
        "reordered": [0, 1],
        "order_timestamp": ["2025-04-01 10:00:00", "2025-04-01 11:00:00"],
        "date": ["2025-04-01", "2025-04-01"]
    }
    expected_df = pd.DataFrame(expected_data)

    # Assert
    pd.testing.assert_frame_equal(
        deduped_df.reset_index(drop=True),
        expected_df.reset_index(drop=True),
        check_dtype=False
    )
    assert len(deduped_df) == 2