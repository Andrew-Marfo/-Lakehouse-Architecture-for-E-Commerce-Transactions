import pytest
import pandas as pd
import boto3
from moto import mock_s3
from unittest import mock

# Mock Spark dependencies in validate_dataframe
with mock.patch('pyspark.sql.DataFrame.filter', return_value=None) as mock_filter, \
     mock.patch('pyspark.sql.DataFrame.count', return_value=0) as mock_count, \
     mock.patch('pyspark.sql.DataFrame.write') as mock_write, \
     mock.patch('pyspark.sql.DataFrame.na') as mock_na, \
     mock.patch('pyspark.sql.DataFrame.select') as mock_select:
    from src.utils.validation import validate_dataframe

@pytest.fixture
def s3_client():
    with mock_s3():
        s3 = boto3.client('s3', region_name='eu-west-1')
        s3.create_bucket(Bucket='ecommerce-lakehouse', CreateBucketConfiguration={'LocationConstraint': 'eu-west-1'})
        yield s3

@pytest.fixture
def sample_data():
    data = {
        "order_id": [1, None, 3, 4],  # Null primary key in second row
        "user_id": [501, 502, None, 504],  # Null required column in third row
        "order_timestamp": ["2025-04-01 10:00:00", "2025-04-01 11:00:00", "2025-04-01 12:00:00", "invalid_timestamp"],  # Invalid timestamp in fourth row
        "date": ["2025-04-01", "2025-04-01", "2025-04-01", "2025-04-01"]
    }
    return pd.DataFrame(data)

# Mock schema for validation
schema = {
    "order_id": {"type": "int", "nullable": False},
    "user_id": {"type": "int", "nullable": False},
    "order_timestamp": {"type": "timestamp", "nullable": False},
    "date": {"type": "string", "nullable": False}
}

def test_validate_dataframe(sample_data, s3_client, mocker):
    # Mock Spark DataFrame methods to simulate validation
    mocker.patch('src.utils.validation.DataFrame.filter', return_value=sample_data.iloc[1:])  # Simulate rejected records
    mocker.patch('src.utils.validation.DataFrame.count', return_value=3)  # Simulate rejected count
    mocker.patch('src.utils.validation.DataFrame.write')  # Mock S3 write
    mocker.patch('src.utils.validation.DataFrame.na', return_value=sample_data)  # Mock na.drop

    # Mock the Spark DataFrame creation (we'll use Pandas for testing)
    primary_key = "order_id"
    required_columns = ["order_id", "user_id", "order_timestamp", "date"]
    rejected_path = "s3://ecommerce-lakehouse/rejected/orders/"

    # Simulate validation by manually applying some logic (since we're using Pandas)
    # In a real Spark environment, this would be handled by validate_dataframe
    validated_df = sample_data.dropna(subset=required_columns)  # Drop rows with nulls
    validated_df = validated_df[validated_df['order_timestamp'].str.contains(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', regex=True)]  # Filter invalid timestamps
    rejected_df = sample_data[~sample_data.index.isin(validated_df.index)]

    # Call validate_dataframe (it will use the mocked methods)
    result_df = validate_dataframe(validated_df, schema, primary_key, required_columns, rejected_path)

    # Check results
    assert len(result_df) == 1  # Only the valid record remains
    assert result_df.iloc[0]["order_id"] == 1

    # Verify rejected records in S3 (mocked)
    rejected_files = s3_client.list_objects_v2(Bucket='ecommerce-lakehouse', Prefix='rejected/orders/')
    assert 'Contents' in rejected_files  # Rejected records were "written"
    assert len(rejected_files['Contents']) > 0