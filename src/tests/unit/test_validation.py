# tests/test_validation.py
import pytest
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from src.utils.validation import validate_dataframe

@pytest.fixture
def products_schema():
    return StructType([
        StructField("product_id", IntegerType(), False),
        StructField("department_id", IntegerType(), False),
        StructField("department", StringType(), False),
        StructField("product_name", StringType(), False)
    ])

def test_validate_dataframe(spark_session, products_schema, tmp_path):
    # Setup test data
    test_data = [
        (1, 101, "Electronics", "Smartphone"),  # valid
        (None, 102, "Clothing", "T-Shirt"),    # invalid (null product_id)
        (2, None, None, "Laptop"),             # invalid (null department_id and department)
        (3, 103, "Home", None)                 # invalid (null product_name)
    ]
    df = spark_session.createDataFrame(test_data, ["product_id", "department_id", "department", "product_name"])
    
    # Run validation
    rejected_path = str(tmp_path)
    required_columns = ["product_id", "department_id", "department", "product_name"]
    validated_df = validate_dataframe(df, products_schema, "product_id", required_columns, rejected_path)
    
    # Assertions
    assert validated_df.count() == 1  # only first record should be valid
    assert validated_df.collect()[0]["product_id"] == 1
    
    # Check rejected records were written (simplified check)
    assert any(tmp_path.iterdir())  # verify something was written to rejected path