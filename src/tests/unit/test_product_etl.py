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