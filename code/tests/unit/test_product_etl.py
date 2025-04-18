import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from code.glue_scripts.product_etl import deduplicate_data

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("TestProductETL").getOrCreate()

def test_deduplicate_data(spark):
    # Define schema
    schema = StructType([
        StructField("product_id", IntegerType(), False),
        StructField("department_id", IntegerType(), False),
        StructField("department", StringType(), False),
        StructField("product_name", StringType(), False)
    ])

    # Create test data with duplicates
    data = [
        (1, 101, "Electronics", "Laptop"),
        (1, 101, "Electronics", "Laptop"),  # Duplicate
        (2, 102, "Books", "Novel")
    ]
    df = spark.createDataFrame(data, schema)

    # Deduplicate
    deduped_df = deduplicate_data(df, "product_id")

    # Expected data
    expected_data = [
        (1, 101, "Electronics", "Laptop"),
        (2, 102, "Books", "Novel")
    ]
    expected_df = spark.createDataFrame(expected_data, schema)

    # Assert
    assert deduped_df.count() == 2
    assert sorted(deduped_df.collect()) == sorted(expected_df.collect())