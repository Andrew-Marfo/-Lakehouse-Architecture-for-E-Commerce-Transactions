import logging
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import StructType

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('validation')


def validate_dataframe(
    df: DataFrame, 
    schema: StructType, 
    primary_key: str, 
    required_columns: list, 
    rejected_path: str
) -> DataFrame:
    """
    Validate the DataFrame: schema enforcement, null primary keys, required columns, valid timestamps.
    Write rejected records to S3.
    """
    try:
        logger.info("Starting validation for DataFrame with primary key: %s", primary_key)

        # Enforce the schema by casting columns to the defined types
        logger.info("Enforcing schema on DataFrame")
        for field in schema.fields:
            df = df.withColumn(field.name, col(field.name).cast(field.dataType))

        # Check for schema mismatches (columns that can't be cast will result in nulls)
        schema_mismatch_df = df.filter(
            reduce(
                lambda x, y: x | y, 
                [col(field.name).isNull() for field in schema.fields if field.name in required_columns]
            )
        )
        if schema_mismatch_df.count() > 0:
            logger.warning("Found %d records with schema mismatches", schema_mismatch_df.count())
            schema_mismatch_df.write.mode("append").csv(rejected_path)
            df = df.na.drop(subset=required_columns)

        # Check for null primary keys
        null_pk_df = df.filter(col(primary_key).isNull())
        if null_pk_df.count() > 0:
            logger.warning("Found %d records with null primary key: %s", null_pk_df.count(), primary_key)
            null_pk_df.write.mode("append").csv(rejected_path)
            df = df.filter(col(primary_key).isNotNull())

        # Check for null required columns
        for column in required_columns:
            null_col_df = df.filter(col(column).isNull())
            if null_col_df.count() > 0:
                logger.warning("Found %d records with null in required column: %s", null_col_df.count(), column)
                null_col_df.write.mode("append").csv(rejected_path)
                df = df.filter(col(column).isNotNull())

        # For orders and order_items, validate timestamps
        if 'order_timestamp' in df.columns:
            invalid_ts_df = df.filter(~col('order_timestamp').cast('timestamp').isNotNull())
            if invalid_ts_df.count() > 0:
                logger.warning("Found %d records with invalid timestamps", invalid_ts_df.count())
                invalid_ts_df.write.mode("append").csv(rejected_path)
                df = df.filter(col('order_timestamp').cast('timestamp').isNotNull())

        logger.info("Validation complete. Remaining records: %d", df.count())
        return df

    except Exception as e:
        logger.error("Error during validation: %s", str(e))
        raise
