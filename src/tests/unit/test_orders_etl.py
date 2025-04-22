import pytest
import pandas as pd
import sys
from unittest import mock

# Mock the awsglue module to prevent SparkContext creation
sys.modules['awsglue'] = mock.MagicMock()
sys.modules['awsglue.utils'] = mock.MagicMock()
sys.modules['awsglue.context'] = mock.MagicMock()
sys.modules['awsglue.job'] = mock.MagicMock()

# Mock the validate_dataframe function to avoid Spark dependencies
with mock.patch('src.utils.validation.validate_dataframe', return_value=None) as mock_validate:
    from src.glue_scripts.orders_etl import deduplicate_data

@pytest.fixture
def sample_data():
    data = {
        "order_num": [1, 1, 2],  # Duplicate order_id
        "order_id": [1001, 1001, 1002],
        "user_id": [501, 501, 502],
        "order_timestamp": ["2025-04-01 10:00:00", "2025-04-01 10:00:00", "2025-04-01 11:00:00"],
        "total_amount": [199.99, 199.99, 49.99],
        "date": ["2025-04-01", "2025-04-01", "2025-04-01"]
    }
    return pd.DataFrame(data)

def test_deduplicate_data(sample_data):
    # Deduplicate using Pandas DataFrame
    deduped_df = deduplicate_data(sample_data, "order_id")

    # Expected data after deduplication
    expected_data = {
        "order_num": [1, 2],
        "order_id": [1001, 1002],
        "user_id": [501, 502],
        "order_timestamp": ["2025-04-01 10:00:00", "2025-04-01 11:00:00"],
        "total_amount": [199.99, 49.99],
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