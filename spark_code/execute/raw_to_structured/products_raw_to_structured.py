from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, 
    current_timestamp,
    trim,
    upper,
    when,
    lit,
    regexp_replace
)
from adapters.data_reader import DeltaTableReader
from adapters.data_writer import DeltaTableWriter
from execute.raw_to_structured.data_quality import DataQualityChecker, get_rejection_reasons
from utils.logger import get_logger

logger = get_logger(__name__)

# Initialize Spark Session
spark_session = (
    SparkSession.builder.appName("ProductsRawToStructured")
    .enableHiveSupport()
    .getOrCreate()
)

# Configuration
RAW_TABLE = "raw.products"
STRUCTURED_TABLE = "structured.products"
REJECTED_TABLE = "structured.products_rejected"

# Valid categories
VALID_CATEGORIES = ["Electronics", "Clothing", "Food", "Books"]

# Initialize readers and quality checker
delta_reader = DeltaTableReader(spark_session)
quality_checker = DataQualityChecker(spark_session)

logger.info("========== Starting Products Raw to Structured ETL ==========")

# Read raw data
logger.info("Reading data from raw.products")
raw_df = delta_reader.read_delta_table(
    table_name=RAW_TABLE,
    columns=[
        "product_id", "product_name", "category", "price",
        "_corrupt_records", "insertion_timestamp"
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
    required_columns=["product_id", "product_name", "category"],
    tag_column="has_null_required"
)

# 3. Check price is positive
logger.info("Step 3: Checking price validity")
valid_df = quality_checker.check_positive_values(
    valid_df,
    numeric_columns=["price"],
    flag_prefix="is_valid_"
)

# 4. Check category is valid
logger.info("Step 4: Checking category validity")
valid_df = valid_df.withColumn(
    "is_valid_category",
    when(col("category").isin(VALID_CATEGORIES), lit("Y")).otherwise(lit("N"))
)

# 5. Check for duplicates
logger.info("Step 5: Checking for duplicates")
valid_df, duplicate_df = quality_checker.check_duplicates(valid_df, key_columns=["product_id"])

# =============================================================================
# SEPARATE VALID AND REJECTED RECORDS
# =============================================================================

logger.info("Separating valid and rejected records")

# Define rejection criteria
rejection_condition = (
    (col("has_null_required") == "Y") |
    (col("is_valid_price") == "N")
)

rejected_quality_df = valid_df.filter(rejection_condition)
clean_df = valid_df.filter(~rejection_condition)

# Add rejection reasons
rejection_checks = {
    "has_null_required": "Required field is null",
    "is_valid_price": "Invalid price (zero, negative, or non-numeric)"
}
rejected_quality_df = get_rejection_reasons(rejected_quality_df, rejection_checks)

# =============================================================================
# TRANSFORM CLEAN DATA FOR STRUCTURED LAYER
# =============================================================================

logger.info("Transforming clean data for structured layer")

structured_df = clean_df.select(
    col("product_id"),
    trim(col("product_name")).alias("product_name"),
    regexp_replace(trim(col("product_name")), r'[^a-zA-Z0-9\s]', '').alias("product_name_cleaned"),
    trim(col("category")).alias("category"),
    col("price"),
    when(col("price") < 50, lit("Budget"))
        .when(col("price") < 200, lit("Mid-Range"))
        .when(col("price") < 500, lit("Premium"))
        .otherwise(lit("Luxury")).alias("price_category"),
    col("is_valid_price"),
    current_timestamp().alias("structured_insertion_timestamp")
)

# =============================================================================
# WRITE TO STRUCTURED LAYER (MERGE)
# =============================================================================

logger.info("Writing clean data to structured.products using merge")
structured_writer = DeltaTableWriter(spark_session, STRUCTURED_TABLE)
structured_writer = DeltaTableWriter(spark_session, STRUCTURED_TABLE)
structured_writer.write_merge(
    df=structured_df,
    merge_col="product_id",
)

# =============================================================================
# WRITE REJECTED RECORDS
# =============================================================================

logger.info("Writing rejected records")

all_rejected_columns = [
    "product_id", "product_name", "category", "price",
    "_corrupt_records"
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

# Duplicate records
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