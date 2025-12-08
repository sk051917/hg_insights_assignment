from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, 
    current_timestamp,
    year,
    month,
    dayofmonth,
    when,
    lit
)
from adapters.data_reader import DeltaTableReader
from adapters.data_writer import DeltaTableWriter
from execute.raw_to_structured.data_quality import DataQualityChecker, get_rejection_reasons
from utils.logger import get_logger

logger = get_logger(__name__)

# Initialize Spark Session
spark_session = (
    SparkSession.builder.appName("OrdersRawToStructured")
    .enableHiveSupport()
    .getOrCreate()
)

# Configuration
RAW_TABLE = "raw.orders"
STRUCTURED_TABLE = "structured.orders"
REJECTED_TABLE = "structured.orders_rejected"

# Initialize readers and quality checker
delta_reader = DeltaTableReader(spark_session)
quality_checker = DataQualityChecker(spark_session)

logger.info("========== Starting Orders Raw to Structured ETL ==========")

# Read raw data
logger.info("Reading data from raw.orders")
raw_df = delta_reader.read_delta_table(
    table_name=RAW_TABLE,
    columns=[
        "order_id", "customer_id", "product_id", "order_date",
        "quantity", "_corrupt_records", "insertion_timestamp"
    ]
)

# =============================================================================
# DATA QUALITY CHECKS
# =============================================================================

# 1. Check for corrupt records
logger.info("Step 1: Checking for corrupt records")
valid_df, corrupt_df = quality_checker.check_corrupt_records(raw_df, "_corrupt_records")

# 2. Check for null values in required columns
logger.info("Step 2: Checking for null values in required columns")
valid_df = quality_checker.check_null_values(
    valid_df,
    required_columns=["order_id", "customer_id", "product_id", "order_date"],
    tag_column="has_null_required"
)

# 3. Check quantity is positive
logger.info("Step 3: Checking quantity validity")
valid_df = quality_checker.check_positive_values(
    valid_df,
    numeric_columns=["quantity"],
    flag_prefix="is_valid_"
)

# 4. Check date range (orders shouldn't be in the future)
logger.info("Step 4: Checking order date validity")
valid_df = quality_checker.check_date_range(
    valid_df,
    date_column="order_date",
    min_date="2020-01-01",
    max_date=None,
    flag_column="is_valid_date"
)

# 5. Check for duplicates
logger.info("Step 5: Checking for duplicates")
valid_df, duplicate_df = quality_checker.check_duplicates(valid_df, key_columns=["order_id"])

# =============================================================================
# SEPARATE VALID AND REJECTED RECORDS
# =============================================================================

logger.info("Separating valid and rejected records")

# Define rejection criteria
rejection_condition = (
    (col("has_null_required") == "Y") |
    (col("is_valid_quantity") == "N") |
    (col("is_valid_date") == "N")
)

rejected_quality_df = valid_df.filter(rejection_condition)
clean_df = valid_df.filter(~rejection_condition)

# Add rejection reasons
rejection_checks = {
    "has_null_required": "Required field is null",
    "is_valid_quantity": "Invalid quantity (zero or negative)",
    "is_valid_date": "Invalid order date"
}
rejected_quality_df = get_rejection_reasons(rejected_quality_df, rejection_checks)

# =============================================================================
# TRANSFORM CLEAN DATA FOR STRUCTURED LAYER
# =============================================================================

logger.info("Transforming clean data for structured layer")

structured_df = clean_df.select(
    col("order_id"),
    col("customer_id"),
    col("product_id"),
    col("order_date"),
    year(col("order_date")).alias("order_year"),
    month(col("order_date")).alias("order_month"),
    dayofmonth(col("order_date")).alias("order_day"),
    col("quantity"),
    col("is_valid_quantity"),
    current_timestamp().alias("structured_insertion_timestamp")
)

# Repartition for optimal write performance
partition_count = max(1, structured_df.count() // 100000)
if partition_count > 1:
    logger.info(f"Repartitioning to {partition_count} partitions")
    structured_df = structured_df.repartition(partition_count)

# =============================================================================
# WRITE TO STRUCTURED LAYER (MERGE)
# =============================================================================

logger.info("Writing clean data to structured.orders using merge")
structured_writer = DeltaTableWriter(spark_session, STRUCTURED_TABLE)
structured_writer.write_merge(
    df=structured_df,
    merge_col="order_id",
)

# =============================================================================
# WRITE REJECTED RECORDS
# =============================================================================

logger.info("Writing rejected records")

all_rejected_columns = [
    "order_id", "customer_id", "product_id", "order_date",
    "quantity", "_corrupt_records",
]

# Corrupt records
if corrupt_df.count() > 0:
    corrupt_df = corrupt_df.select(*all_rejected_columns)
    logger.info(f"corrupt_df: {corrupt_df.columns}")
    rejected_writer = DeltaTableWriter(spark_session, REJECTED_TABLE)
    rejected_writer.write_rejected_records(
        df=corrupt_df,
        rejection_reason="Corrupt record - parsing failed",
    )

if duplicate_df.count() > 0:
    logger.info("Writing duplicate records to rejected table")
    duplicate_rejected = duplicate_df.select(*all_rejected_columns)
    logger.info(f"duplicate_rejected: {duplicate_rejected.columns}")

    duplicate_rejected_writer =DeltaTableWriter(spark_session, REJECTED_TABLE)
    duplicate_rejected_writer.write_rejected_records(
        df=duplicate_rejected,
        rejection_reason="Duplicate record",
    )

# Quality failed records
if rejected_quality_df.count() > 0:
    logger.info("Writing quality failed records to rejected table")
    rejected_quality_df = rejected_quality_df.select(*all_rejected_columns)
    logger.info(f"rejected_quality_df: {rejected_quality_df.columns}")
    
    dq_rejected_writer = DeltaTableWriter(spark_session, REJECTED_TABLE)
    dq_rejected_writer.write_rejected_records(
        df=rejected_quality_df,
        rejection_reason="Data quality check failed",
    )

spark_session.stop()