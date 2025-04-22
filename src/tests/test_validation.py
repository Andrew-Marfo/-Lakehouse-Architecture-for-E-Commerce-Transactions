import pytest
import pandas as pd
import boto3
import sys
import re
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
schema_dict = {
    "order_id": {"type": "int", "nullable": False},
    "user_id": {"type": "int", "nullable": False},
    "order_timestamp": {"type": "timestamp", "nullable": False},
    "date": {"type": "string", "nullable": False}
}

# Mock StructType and StructField for schema
class MockStructField:
    def __init__(self, name, dataType):
        self.name = name
        self.dataType = dataType

class MockStructType:
    def __init__(self, fields):
        self.fields = fields

# Create a mocked schema
mock_schema = MockStructType([
    MockStructField(name, schema_dict[name]["type"])
    for name in schema_dict
])

class MockWrite:
    def __init__(self, df):
        self.df = df
    
    def mode(self, mode):
        return self
    
    def csv(self, path):
        # In a real test, you might want to verify the path or contents
        pass

class MockNA:
    def __init__(self, df):
        self.df = df
    
    def drop(self, subset):
        return MockDataFrame(self.df.dropna(subset=subset))

# Mock DataFrame class to simulate Spark DataFrame behavior with Pandas
class MockDataFrame:
    def __init__(self, df):
        self.df = df
        self._col_calls = []

    @property
    def columns(self):
        return list(self.df.columns)

    def withColumn(self, colName, col):
        # Simulate schema enforcement (no-op for Pandas in this test, as we're using Pandas types)
        return self

    def filter(self, condition):
        # Handle mocked col().isNull() and col().cast('timestamp').isNotNull()
        condition_str = str(condition)
        
        if "isNull()" in condition_str:
            # Get the last column that was called with col()
            if hasattr(sys.modules['pyspark.sql.functions'].col, 'call_args'):
                column = sys.modules['pyspark.sql.functions'].col.call_args[0][0]
                return MockDataFrame(self.df[self.df[column].isna()])
        
        elif "cast('timestamp')" in condition_str:
            # Filter for invalid timestamps
            valid_pattern = r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}'
            mask = self.df['order_timestamp'].apply(
                lambda x: bool(re.match(valid_pattern, str(x))))
            return MockDataFrame(self.df[~mask])
        
        return self

    def count(self):
        return len(self.df)

    def write(self):
        return MockWrite(self.df)

    def na(self):
        return MockNA(self.df)

def test_enforce_schema(sample_data, mocker):
    # Mock Spark DataFrame
    mocker.patch('src.utils.validation.DataFrame', MockDataFrame)
    
    input_df = MockDataFrame(sample_data)
    result_df = enforce_schema(input_df, mock_schema)
    
    assert result_df.count() == len(sample_data)
    pd.testing.assert_frame_equal(result_df.df.reset_index(drop=True), sample_data.reset_index(drop=True))

def test_reject_schema_mismatches(sample_data, s3_client, mocker):
    # Mock Spark DataFrame and col function
    mocker.patch('src.utils.validation.DataFrame', MockDataFrame)
    mock_col = mocker.patch('src.utils.validation.col')
    mock_col.side_effect = lambda x: mock.MagicMock(isNull=lambda: f"col({x}).isNull()")
    
    required_columns = ["order_id", "user_id", "order_timestamp", "date"]
    rejected_path = "s3://ecommerce-lakehouse/rejected/orders/"
    
    input_df = MockDataFrame(sample_data)
    result_df = reject_schema_mismatches(input_df, mock_schema, required_columns, rejected_path)
    
    # After dropping nulls in required columns, only rows with no nulls in required columns should remain
    expected_df = sample_data.dropna(subset=required_columns)
    assert result_df.count() == len(expected_df)
    pd.testing.assert_frame_equal(result_df.df.reset_index(drop=True), expected_df.reset_index(drop=True))

def test_reject_null_primary_keys(sample_data, s3_client, mocker):
    # Mock Spark DataFrame and col function
    mocker.patch('src.utils.validation.DataFrame', MockDataFrame)
    mock_col = mocker.patch('src.utils.validation.col')
    mock_col.side_effect = lambda x: mock.MagicMock(isNull=lambda: f"col({x}).isNull()")
    
    primary_key = "order_id"
    rejected_path = "s3://ecommerce-lakehouse/rejected/orders/"
    
    input_df = MockDataFrame(sample_data)
    result_df = reject_null_primary_keys(input_df, primary_key, rejected_path)
    
    # Rows with null primary key should be removed
    expected_df = sample_data[sample_data[primary_key].notna()]
    assert result_df.count() == len(expected_df)
    pd.testing.assert_frame_equal(result_df.df.reset_index(drop=True), expected_df.reset_index(drop=True))

def test_reject_null_required_columns(sample_data, s3_client, mocker):
    # Mock Spark DataFrame and col function
    mocker.patch('src.utils.validation.DataFrame', MockDataFrame)
    mock_col = mocker.patch('src.utils.validation.col')
    mock_col.side_effect = lambda x: mock.MagicMock(isNull=lambda: f"col({x}).isNull()")
    
    required_columns = ["order_id", "user_id", "order_timestamp", "date"]
    rejected_path = "s3://ecommerce-lakehouse/rejected/orders/"
    
    input_df = MockDataFrame(sample_data)
    result_df = reject_null_required_columns(input_df, required_columns, rejected_path)
    
    # Rows with nulls in required columns should be removed
    expected_df = sample_data.dropna(subset=required_columns)
    assert result_df.count() == len(expected_df)
    pd.testing.assert_frame_equal(result_df.df.reset_index(drop=True), expected_df.reset_index(drop=True))

def test_reject_invalid_timestamps(sample_data, s3_client, mocker):
    # Mock Spark DataFrame and col function
    mocker.patch('src.utils.validation.DataFrame', MockDataFrame)
    mock_col = mocker.patch('src.utils.validation.col')
    mock_col.side_effect = lambda x: mock.MagicMock(
        cast=lambda y: mock.MagicMock(isNotNull=lambda: f"col({x}).cast({y}).isNotNull()")
    )
    
    rejected_path = "s3://ecommerce-lakehouse/rejected/orders/"
    
    input_df = MockDataFrame(sample_data)
    result_df = reject_invalid_timestamps(input_df, rejected_path)
    
    # Rows with invalid timestamps should be removed
    valid_pattern = r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}'
    expected_df = sample_data[sample_data['order_timestamp']].apply(
        lambda x: bool(re.match(valid_pattern, str(x))))
    assert result_df.count() == len(expected_df)
    pd.testing.assert_frame_equal(result_df.df.reset_index(drop=True), expected_df.reset_index(drop=True))

def test_validate_dataframe(sample_data, s3_client, mocker):
    # Mock Spark DataFrame and col function
    mocker.patch('src.utils.validation.DataFrame', MockDataFrame)
    mock_col = mocker.patch('src.utils.validation.col')
    mock_col.side_effect = lambda x: mock.MagicMock(
        isNull=lambda: f"col({x}).isNull()",
        cast=lambda y: mock.MagicMock(isNotNull=lambda: f"col({x}).cast({y}).isNotNull()")
    )
    
    primary_key = "order_id"
    required_columns = ["order_id", "user_id", "order_timestamp", "date"]
    rejected_path = "s3://ecommerce-lakehouse/rejected/orders/"
    
    input_df = MockDataFrame(sample_data)
    result_df = validate_dataframe(input_df, mock_schema, primary_key, required_columns, rejected_path)
    
    # After all validations, only valid records should remain
    valid_pattern = r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}'
    expected_df = sample_data[
        (sample_data['order_id'].notna()) &
        (sample_data['user_id'].notna()) &
        (sample_data['order_timestamp'].apply(lambda x: bool(re.match(valid_pattern, str(x)))))
    ]
    assert result_df.count() == len(expected_df)
    pd.testing.assert_frame_equal(result_df.df.reset_index(drop=True), expected_df.reset_index(drop=True))