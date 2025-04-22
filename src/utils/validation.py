import logging
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import StructType

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('validation')

# Enforce the schema by casting columns to the defined types
def enforce_schema(df: DataFrame, schema: StructType) -> DataFrame:
    logger.info("Enforcing schema on DataFrame")
    for field in schema.fields:
        df = df.withColumn(field.name, col(field.name).cast(field.dataType))
    return df

# Check for schema mismatches (columns that can't be cast result in nulls) and reject those records..
def reject_schema_mismatches(df: DataFrame, schema: StructType, required_columns: list, rejected_path: str) -> DataFrame:
    schema_mismatch_df = df.filter(
        reduce(lambda x, y: x | y, [col(field.name).isNull() for field in schema.fields if field.name in required_columns])
    )
    mismatch_count = schema_mismatch_df.count()
    if mismatch_count > 0:
        logger.warning("Found %d records with schema mismatches", mismatch_count)
        schema_mismatch_df.write.mode("append").csv(rejected_path)
        df = df.na.drop(subset=required_columns)
    return df

# Check for null primary keys and reject those records.
def reject_null_primary_keys(df: DataFrame, primary_key: str, rejected_path: str) -> DataFrame:
    null_pk_df = df.filter(col(primary_key).isNull())
    null_pk_count = null_pk_df.count()
    if null_pk_count > 0:
        logger.warning("Found %d records with null primary key: %s", null_pk_count, primary_key)
        null_pk_df.write.mode("append").csv(rejected_path)
        df = df.filter(col(primary_key).isNotNull())
    return df

# Check for nulls in required columns and reject those records.
def reject_null_required_columns(df: DataFrame, required_columns: list, rejected_path: str) -> DataFrame:
    for column in required_columns:
        null_col_df = df.filter(col(column).isNull())
        null_col_count = null_col_df.count()
        if null_col_count > 0:
            logger.warning("Found %d records with null in required column: %s", null_col_count, column)
            null_col_df.write.mode("append").csv(rejected_path)
            df = df.filter(col(column).isNotNull())
    return df

# Validate timestamps in the 'order_timestamp' column (if present) and reject invalid records.
def reject_invalid_timestamps(df: DataFrame, rejected_path: str) -> DataFrame:
    if 'order_timestamp' in df.columns:
        invalid_ts_df = df.filter(~col('order_timestamp').cast('timestamp').isNotNull())
        invalid_ts_count = invalid_ts_df.count()
        if invalid_ts_count > 0:
            logger.warning("Found %d records with invalid timestamps", invalid_ts_count)
            invalid_ts_df.write.mode("append").csv(rejected_path)
            df = df.filter(col('order_timestamp').cast('timestamp').isNotNull())
    return df

# Validate the DataFrame: schema enforcement, null primary keys, required columns, valid timestamps. Write rejected records to S3.
def validate_dataframe(df: DataFrame, schema: StructType, primary_key: str, required_columns: list, rejected_path: str) -> DataFrame:
    try:
        logger.info("Starting validation for DataFrame with primary key: %s", primary_key)

        # Step 1: Enforce schema
        df = enforce_schema(df, schema)

        # Step 2: Reject schema mismatches
        df = reject_schema_mismatches(df, schema, required_columns, rejected_path)

        # Step 3: Reject null primary keys
        df = reject_null_primary_keys(df, primary_key, rejected_path)

        # Step 4: Reject null required columns
        df = reject_null_required_columns(df, required_columns, rejected_path)

        # Step 5: Reject invalid timestamps
        df = reject_invalid_timestamps(df, rejected_path)

        logger.info("Validation complete. Remaining records: %d", df.count())
        return df

    except Exception as e:
        logger.error("Error during validation: %s", str(e))
        raise