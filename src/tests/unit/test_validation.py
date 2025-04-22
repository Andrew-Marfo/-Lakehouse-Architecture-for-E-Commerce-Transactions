# tests/unit/test_validation.py
from src.utils.validation import validate_dataframe

def test_validation_rejects_null_pk(spark, sample_product_data):
    validated_df = validate_dataframe(
        df=sample_product_data,
        schema=sample_product_data.schema,
        primary_key="product_id",
        required_columns=["product_id", "department"],
        rejected_path="dummy_path"
    )
    assert validated_df.count() == 2  # Only valid records remain

def test_validation_rejects_null_required_columns(spark):
    data = [(1, "Valid"), (2, None)]
    df = spark.createDataFrame(data, ["id", "required_col"])
    
    validated = validate_dataframe(
        df=df,
        schema=df.schema,
        primary_key="id",
        required_columns=["required_col"],
        rejected_path="dummy_path"
    )
    assert validated.count() == 1