# tests/conftest.py
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("pytest") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

@pytest.fixture
def sample_product_data(spark):
    data = [
        (1, 101, "Electronics", "Smartphone"),
        (2, 102, "Clothing", "T-Shirt"),
        (None, 103, "Home", "Couch")  # Invalid record
    ]
    return spark.createDataFrame(data, ["product_id", "department_id", "department", "product_name"])