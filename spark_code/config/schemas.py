from pyspark.sql.types import (
    StructType, 
    StructField, 
    IntegerType, 
    StringType, 
    DateType, 
    DoubleType,
    TimestampType,
    LongType
)

# =============================================================================
# RAW LAYER SCHEMAS (Bronze)
# =============================================================================

CUSTOMER_INPUT_CSV_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("signup_date", DateType(), True),
        StructField("country_code", StringType(), True),
        StructField("department_code", StringType(), True),
        StructField("_corrupt_records", StringType(), True),
    ]
)


ORDERS_INPUT_CSV_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("order_date", DateType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("_corrupt_records", StringType(), True),
])


PRODUCTS_INPUT_CSV_SCHEMA = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("_corrupt_records", StringType(), True),
])