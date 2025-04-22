# tests/conftest.py
import pytest
from pyspark.sql import SparkSession
from moto import mock_s3
import boto3

@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder \
        .appName("pytest") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture
def mock_s3_bucket(spark_session):
    with mock_s3():
        s3 = boto3.client('s3')
        s3.create_bucket(Bucket='test-bucket')
        yield s3