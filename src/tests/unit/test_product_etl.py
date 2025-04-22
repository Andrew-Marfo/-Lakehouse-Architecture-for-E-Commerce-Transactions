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
    from src.glue_scripts.product_etl import deduplicate_data

@pytest.fixture
def sample_data():
    data = {
        "product_id": [1, 1, 2],  # Duplicate product_id
        "department_id": [101, 101, 102],
        "department": ["Electronics", "Electronics", "Books"],
        "product_name": ["Laptop", "Laptop", "Novel"]
    }
    return pd.DataFrame(data)

def test_deduplicate_data(sample_data):
    # Deduplicate using Pandas DataFrame
    deduped_df = deduplicate_data(sample_data, "product_id")

    # Expected data after deduplication
    expected_data = {
        "product_id": [1, 2],
        "department_id": [101, 102],
        "department": ["Electronics", "Books"],
        "product_name": ["Laptop", "Novel"]
    }
    expected_df = pd.DataFrame(expected_data)

    # Assert
    pd.testing.assert_frame_equal(
        deduped_df.reset_index(drop=True),
        expected_df.reset_index(drop=True),
        check_dtype=False
    )
    assert len(deduped_df) == 2