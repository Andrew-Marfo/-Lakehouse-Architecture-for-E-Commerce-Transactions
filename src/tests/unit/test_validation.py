import pytest
import pandas as pd
import boto3
from moto import mock_s3
from unittest import mock

# Mock pyspark imports to prevent Spark-related errors
sys.modules['pyspark'] = mock.MagicMock()
sys.modules['pyspark.sql'] = mock.MagicMock()
sys.modules['pyspark.sql.functions'] = mock.MagicMock()
sys.modules['pyspark.sql.types'] = mock.MagicMock()

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

# Mock schema for validation (simplified for testing)
schema = {
    "order_id": {"type": "int", "nullable": False},
    "user_id": {"type": "int", "nullable": False},
    "order_timestamp": {"type": "timestamp", "nullable": False},
    "date": {"type": "string", "nullable": False}
}

def test_validate_dataframe(sample_data, s3_client, mocker):
    # Mock Spark DataFrame methods to simulate validation behavior
    class MockDataFrame:
        def __init__(self, df):
            self.df = df

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

    # Mock DataFrame to use Pandas under the hood
    mocker.patch('src.utils.validation.DataFrame', MockDataFrame)

    primary_key = "order_id"
    required_columns = ["order_id", "user_id", "order_timestamp", "date"]
    rejected_path = "s3://ecommerce-lakehouse/rejected/orders/"

    # Call validate_dataframe with the mocked DataFrame
    input_df = MockDataFrame(sample_data)
    validated_df = validate_dataframe(input_df, schema, primary_key, required_columns, rejected_path)

    # Check results
    assert validated_df.count() == 1  # Only the valid record remains
    assert validated_df.df.iloc[0]["order_id"] == 1

    # Verify rejected records in S3 (mocked)
    rejected_files = s3_client.list_objects_v2(Bucket='ecommerce-lakehouse', Prefix='rejected/orders/')
    assert 'Contents' in rejected_files  # Rejected records were "written"
    assert len(rejected_files['Contents']) > 0