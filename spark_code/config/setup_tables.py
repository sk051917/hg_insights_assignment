from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("DeltaSetup")
    .enableHiveSupport()
    .getOrCreate()
)


print("========== Creating databases ==========")

# =============================================================================
# CREATE DATABASES
# =============================================================================
spark.sql("CREATE DATABASE IF NOT EXISTS raw")
spark.sql("CREATE DATABASE IF NOT EXISTS structured")

# =============================================================================
# RAW LAYER TABLES (Bronze) - Append Only
# =============================================================================

print("========== Creating raw layer tables ==========")
spark.sql(
    """
    CREATE TABLE IF NOT EXISTS raw.customers_exposed (
        customer_id STRING,
        first_name STRING,
        last_name STRING,
        email STRING,
        phone_number STRING,
        signup_date DATE,
        country_code STRING,
        department_code STRING,
        _corrupt_records STRING,
        insertion_timestamp TIMESTAMP
    ) USING DELTA
"""
)

spark.sql(
    """
    CREATE TABLE IF NOT EXISTS raw.orders (
        order_id STRING,
        customer_id STRING,
        product_id STRING,
        order_date DATE,
        quantity INT,
        _corrupt_records STRING,
        insertion_timestamp TIMESTAMP
    ) USING DELTA
"""
)

spark.sql(
    """
    CREATE TABLE IF NOT EXISTS raw.products (
        product_id STRING,
        product_name STRING,
        category STRING,
        price DOUBLE,
        _corrupt_records STRING,
        insertion_timestamp TIMESTAMP
    ) USING DELTA
"""
)

# =============================================================================
# STRUCTURED LAYER TABLES (Silver) - Merge/Upsert
# =============================================================================
print("========== Creating structured layer tables ==========")
# Structured Customers Table
spark.sql(
    """
    CREATE TABLE IF NOT EXISTS structured.customers (
        customer_id STRING NOT NULL,
        first_name STRING,
        last_name STRING,
        email STRING,
        phone_number STRING,
        signup_date DATE,
        country_code STRING,
        department_code STRING,
        is_valid_country STRING,
        is_valid_department STRING,
        structured_insertion_timestamp TIMESTAMP
    ) USING DELTA
"""
)

# Structured Orders Table
spark.sql(
    """
    CREATE TABLE IF NOT EXISTS structured.orders (
        order_id STRING NOT NULL,
        customer_id STRING,
        product_id STRING,
        order_date DATE,
        order_year INT,
        order_month INT,
        order_day INT,
        quantity INT,
        is_valid_quantity STRING,
        structured_insertion_timestamp TIMESTAMP
    ) USING DELTA
"""
)

# Structured Products Table
spark.sql(
    """
    CREATE TABLE IF NOT EXISTS structured.products (
        product_id STRING NOT NULL,
        product_name STRING,
        product_name_cleaned STRING,
        category STRING,
        price DOUBLE,
        price_category STRING,
        is_valid_price STRING,
        structured_insertion_timestamp TIMESTAMP
    ) USING DELTA
"""
)

# =============================================================================
# REJECTED/QUARANTINE TABLES (Silver)
# =============================================================================

# Rejected Customers Table
spark.sql(
    """
    CREATE TABLE IF NOT EXISTS structured.customers_rejected (
        customer_id STRING,
        first_name STRING,
        last_name STRING,
        email STRING,
        phone_number STRING,
        signup_date DATE,
        country_code STRING,
        department_code STRING,
        _corrupt_records STRING,
        rejection_reason STRING,
        rejected_at TIMESTAMP
    ) USING DELTA
"""
)

# Rejected Orders Table
spark.sql(
    """
    CREATE TABLE IF NOT EXISTS structured.orders_rejected (
        order_id STRING,
        customer_id STRING,
        product_id STRING,
        order_date DATE,
        quantity INT,
        _corrupt_records STRING,
        rejection_reason STRING,
        rejected_at TIMESTAMP
    ) USING DELTA
"""
)

# Rejected Products Table
spark.sql(
    """
    CREATE TABLE IF NOT EXISTS structured.products_rejected (
        product_id STRING,
        product_name STRING,
        category STRING,
        price DOUBLE,
        _corrupt_records STRING,
        rejection_reason STRING,
        rejected_at TIMESTAMP
    ) USING DELTA
"""
)

print("All tables and views created successfully!")

spark.stop()