import pytest
import pandas as pd
import boto3
import sys
from moto import mock_s3
from unittest import mock

# Mock pyspark imports to prevent Spark-related errors
sys.modules['pyspark'] = mock.MagicMock()
sys.modules['pyspark.sql'] = mock.MagicMock()
sys.modules['pyspark.sql.functions'] = mock.MagicMock()
sys.modules['pyspark.sql.types'] = mock.MagicMock()

from src.utils.validation import (
    enforce_schema,
    reject_schema_mismatches,
    reject_null_primary_keys,
    reject_null_required_columns,
    reject_invalid_timestamps,
    validate_dataframe
)

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

# Mock schema for validation (simplified for testing)
schema = {
    "order_id": {"type": "int", "nullable": False},
    "user_id": {"type": "int", "nullable": False},
    "order_timestamp": {"type": "timestamp", "nullable": False},
    "date": {"type": "string", "nullable": False}
}

# Mock DataFrame class to simulate Spark DataFrame behavior with Pandas
class MockDataFrame:
    def __init__(self, df):
        self.df = df

    def withColumn(self, colName, col):
        # Simulate schema enforcement (no-op for Pandas in this test, as we're using Pandas types)
        return self

    def filter(self, condition):
        # Simulate filtering for nulls and invalid timestamps using Pandas
        if "isNull()" in str(condition):
            column = str(condition).split('.')[0].split('(')[1]
            return MockDataFrame(self.df[self.df[column].isna()])
        elif "cast('timestamp')" in str(condition):
            return MockDataFrame(self.df[~self.df['order_timestamp'].str.contains(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', regex=True)])
        return self

    def count(self):
        return len(self.df)

    def write(self):
        class MockWrite:
            def mode(self, mode):
                return self
            def csv(self, path):
                pass
        return MockWrite()

    def na(self):
        class MockNA:
            def drop(self, subset):
                return MockDataFrame(self.df.dropna(subset=subset))
        return MockNA()

def test_enforce_schema(sample_data, mocker):
    # Mock Spark DataFrame
    mocker.patch('src.utils.validation.DataFrame', MockDataFrame)
    
    input_df = MockDataFrame(sample_data)
    result_df = enforce_schema(input_df, schema)
    
    # Since enforce_schema casts columns, and we're using Pandas, the DataFrame should remain unchanged
    # In a real Spark environment, this would cast columns to the defined types
    assert result_df.count() == len(sample_data)
    pd.testing.assert_frame_equal(result_df.df.reset_index(drop=True), sample_data.reset_index(drop=True))

def test_reject_schema_mismatches(sample_data, s3_client, mocker):
    # Mock Spark DataFrame
    mocker.patch('src.utils.validation.DataFrame', MockDataFrame)
    
    required_columns = ["order_id", "user_id", "order_timestamp", "date"]
    rejected_path = "s3://ecommerce-lakehouse/rejected/orders/"
    
    input_df = MockDataFrame(sample_data)
    result_df = reject_schema_mismatches(input_df, schema, required_columns, rejected_path)
    
    # Schema mismatches are simulated by nulls in required columns
    # After dropping nulls in required columns, only rows with no nulls in required columns should remain
    expected_df = sample_data.dropna(subset=required_columns)
    assert result_df.count() == len(expected_df)
    pd.testing.assert_frame_equal(result_df.df.reset_index(drop=True), expected_df.reset_index(drop=True))
    
    # Verify rejected records in S3
    rejected_files = s3_client.list_objects_v2(Bucket='ecommerce-lakehouse', Prefix='rejected/orders/')
    assert 'Contents' in rejected_files
    assert len(rejected_files['Contents']) > 0

def test_reject_null_primary_keys(sample_data, s3_client, mocker):
    # Mock Spark DataFrame
    mocker.patch('src.utils.validation.DataFrame', MockDataFrame)
    
    primary_key = "order_id"
    rejected_path = "s3://ecommerce-lakehouse/rejected/orders/"
    
    input_df = MockDataFrame(sample_data)
    result_df = reject_null_primary_keys(input_df, primary_key, rejected_path)
    
    # Rows with null primary key should be removed
    expected_df = sample_data[sample_data[primary_key].notna()]
    assert result_df.count() == len(expected_df)
    pd.testing.assert_frame_equal(result_df.df.reset_index(drop=True), expected_df.reset_index(drop=True))
    
    # Verify rejected records in S3
    rejected_files = s3_client.list_objects_v2(Bucket='ecommerce-lakehouse', Prefix='rejected/orders/')
    assert 'Contents' in rejected_files
    assert len(rejected_files['Contents']) > 0

def test_reject_null_required_columns(sample_data, s3_client, mocker):
    # Mock Spark DataFrame
    mocker.patch('src.utils.validation.DataFrame', MockDataFrame)
    
    required_columns = ["order_id", "user_id", "order_timestamp", "date"]
    rejected_path = "s3://ecommerce-lakehouse/rejected/orders/"
    
    input_df = MockDataFrame(sample_data)
    result_df = reject_null_required_columns(input_df, required_columns, rejected_path)
    
    # Rows with nulls in required columns should be removed
    expected_df = sample_data.dropna(subset=required_columns)
    assert result_df.count() == len(expected_df)
    pd.testing.assert_frame_equal(result_df.df.reset_index(drop=True), expected_df.reset_index(drop=True))
    
    # Verify rejected records in S3
    rejected_files = s3_client.list_objects_v2(Bucket='ecommerce-lakehouse', Prefix='rejected/orders/')
    assert 'Contents' in rejected_files
    assert len(rejected_files['Contents']) > 0

def test_reject_invalid_timestamps(sample_data, s3_client, mocker):
    # Mock Spark DataFrame
    mocker.patch('src.utils.validation.DataFrame', MockDataFrame)
    
    rejected_path = "s3://ecommerce-lakehouse/rejected/orders/"
    
    input_df = MockDataFrame(sample_data)
    result_df = reject_invalid_timestamps(input_df, rejected_path)
    
    # Rows with invalid timestamps should be removed
    expected_df = sample_data[sample_data['order_timestamp'].str.contains(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', regex=True)]
    assert result_df.count() == len(expected_df)
    pd.testing.assert_frame_equal(result_df.df.reset_index(drop=True), expected_df.reset_index(drop=True))
    
    # Verify rejected records in S3
    rejected_files = s3_client.list_objects_v2(Bucket='ecommerce-lakehouse', Prefix='rejected/orders/')
    assert 'Contents' in rejected_files
    assert len(rejected_files['Contents']) > 0

def test_validate_dataframe(sample_data, s3_client, mocker):
    # Mock Spark DataFrame
    mocker.patch('src.utils.validation.DataFrame', MockDataFrame)
    
    primary_key = "order_id"
    required_columns = ["order_id", "user_id", "order_timestamp", "date"]
    rejected_path = "s3://ecommerce-lakehouse/rejected/orders/"
    
    input_df = MockDataFrame(sample_data)
    result_df = validate_dataframe(input_df, schema, primary_key, required_columns, rejected_path)
    
    # After all validations, only valid records should remain
    expected_df = sample_data[
        (sample_data['order_id'].notna()) &
        (sample_data['user_id'].notna()) &
        (sample_data['order_timestamp'].str.contains(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', regex=True))
    ]
    assert result_df.count() == len(expected_df)
    pd.testing.assert_frame_equal(result_df.df.reset_index(drop=True), expected_df.reset_index(drop=True))
    
    # Verify rejected records in S3
    rejected_files = s3_client.list_objects_v2(Bucket='ecommerce-lakehouse', Prefix='rejected/orders/')
    assert 'Contents' in rejected_files
    assert len(rejected_files['Contents']) > 0