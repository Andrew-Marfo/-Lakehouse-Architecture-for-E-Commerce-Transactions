import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DoubleType
import sys
from unittest import mock
sys.modules['awsglue'] = mock.MagicMock()
sys.modules['awsglue.utils'] = mock.MagicMock()
sys.modules['awsglue.context'] = mock.MagicMock()
sys.modules['awsglue.job'] = mock.MagicMock()
from src.glue_scripts.orders_etl import deduplicate_data


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("TestOrdersETL").getOrCreate()


def test_deduplicate_data(spark):
    # Define schema
    schema = StructType([
        StructField("order_num", IntegerType(), False),
        StructField("order_id", IntegerType(), False),
        StructField("user_id", IntegerType(), False),
        StructField("order_timestamp", TimestampType(), False),
        StructField("total_amount", DoubleType(), False),
        StructField("date", StringType(), False)
    ])

    # Create test data with duplicates
    data = [
        (1, 1001, 501, "2025-04-01 10:00:00", 199.99, "2025-04-01"),
        (1, 1001, 501, "2025-04-01 10:00:00", 199.99, "2025-04-01"),  # Duplicate
        (2, 1002, 502, "2025-04-01 11:00:00", 49.99, "2025-04-01")
    ]
    df = spark.createDataFrame(data, schema)

    # Deduplicate
    deduped_df = deduplicate_data(df, "order_id")

    # Expected data
    expected_data = [
        (1, 1001, 501, "2025-04-01 10:00:00", 199.99, "2025-04-01"),
        (2, 1002, 502, "2025-04-01 11:00:00", 49.99, "2025-04-01")
    ]
    expected_df = spark.createDataFrame(expected_data, schema)

    # Assert
    assert deduped_df.count() == 2
    assert sorted(deduped_df.collect()) == sorted(expected_df.collect())
    