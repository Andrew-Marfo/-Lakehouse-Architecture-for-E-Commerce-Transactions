# tests/test_validation.py
import pytest
from pyspark.sql import SparkSession
from src.utils.validation import validate_dataframe

@pytest.fixture
def spark():
    return SparkSession.builder \
        .appName("pytest") \
        .getOrCreate()

def test_validate_dataframe_rejects_null_pk(spark):
    data = [(1, "Electronics"), (None, "Clothing")]
    df = spark.createDataFrame(data, ["product_id", "department"])
    
    validated_df = validate_dataframe(
        df=df,
        schema=df.schema,
        primary_key="product_id",
        required_columns=["product_id"],
        rejected_path="dummy_path"
    )
    
    assert validated_df.count() == 1
    assert validated_df.collect()[0]["product_id"] == 1

def test_validate_dataframe_rejects_null_required_columns(spark):
    data = [(1, "Electronics"), (2, None)]
    df = spark.createDataFrame(data, ["product_id", "department"])
    
    validated_df = validate_dataframe(
        df=df,
        schema=df.schema,
        primary_key="product_id",
        required_columns=["department"],
        rejected_path="dummy_path"
    )
    
    assert validated_df.count() == 1