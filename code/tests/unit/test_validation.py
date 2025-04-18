import pytest
import boto3
from moto import mock_s3
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from code.utils.validation import validate_dataframe


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("TestValidation").getOrCreate()


@pytest.fixture
def s3_client():
    with mock_s3():
        s3 = boto3.client('s3', region_name='eu-west-1')
        s3.create_bucket(Bucket='ecommerce-lakehouse', CreateBucketConfiguration={'LocationConstraint': 'eu-west-1'})
        yield s3


def test_validate_dataframe(spark, s3_client):
    # Define schema
    schema = StructType([
        StructField("order_id", IntegerType(), False),
        StructField("user_id", IntegerType(), False),
        StructField("order_timestamp", TimestampType(), False),
        StructField("date", StringType(), False)
    ])

    # Create test data with issues
    data = [
        (1, 501, "2025-04-01 10:00:00", "2025-04-01"),  # Valid
        (None, 502, "2025-04-01 11:00:00", "2025-04-01"),  # Null primary key
        (3, None, "2025-04-01 12:00:00", "2025-04-01"),  # Null required column
        (4, 504, "invalid_timestamp", "2025-04-01")  # Invalid timestamp
    ]
    df = spark.createDataFrame(data, schema)

    # Define parameters
    primary_key = "order_id"
    required_columns = ["order_id", "user_id", "order_timestamp", "date"]
    rejected_path = "s3://ecommerce-lakehouse/rejected/orders/"

    # Validate DataFrame
    validated_df = validate_dataframe(df, schema, primary_key, required_columns, rejected_path)

    # Check results
    assert validated_df.count() == 1  # Only the valid record remains
    assert validated_df.collect()[0]["order_id"] == 1

    # Verify rejected records in S3
    rejected_files = s3_client.list_objects_v2(Bucket='ecommerce-lakehouse', Prefix='rejected/orders/')
    assert 'Contents' in rejected_files  # Rejected records were written
    assert len(rejected_files['Contents']) > 0