import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from src.glue_scripts.order_items_etl import deduplicate_data


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("TestOrderItemsETL").getOrCreate()


def test_deduplicate_data(spark):
    # Define schema
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("order_id", IntegerType(), False),
        StructField("user_id", IntegerType(), False),
        StructField("days_since_prior_order", IntegerType(), True),
        StructField("product_id", IntegerType(), False),
        StructField("add_to_cart_order", IntegerType(), False),
        StructField("reordered", IntegerType(), False),
        StructField("order_timestamp", TimestampType(), False),
        StructField("date", StringType(), False)
    ])

    # Create test data with duplicates
    data = [
        (1, 1001, 501, 30, 101, 1, 0, "2025-04-01 10:00:00", "2025-04-01"),
        (1, 1001, 501, 30, 101, 1, 0, "2025-04-01 10:00:00", "2025-04-01"),  # Duplicate
        (2, 1002, 502, 15, 102, 2, 1, "2025-04-01 11:00:00", "2025-04-01")
    ]
    df = spark.createDataFrame(data, schema)

    # Deduplicate
    deduped_df = deduplicate_data(df, "id")

    # Expected data
    expected_data = [
        (1, 1001, 501, 30, 101, 1, 0, "2025-04-01 10:00:00", "2025-04-01"),
        (2, 1002, 502, 15, 102, 2, 1, "2025-04-01 11:00:00", "2025-04-01")
    ]
    expected_df = spark.createDataFrame(expected_data, schema)

    # Assert
    assert deduped_df.count() == 2
    assert sorted(deduped_df.collect()) == sorted(expected_df.collect())
    