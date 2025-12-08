from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, 
    current_timestamp, 
    concat_ws, 
    split, 
    element_at,
    trim,
    upper,
    when,
    lit,
    sha2
)
from adapters.data_reader import DeltaTableReader, StaticDataReader
from adapters.data_writer import DeltaTableWriter
from execute.raw_to_structured.data_quality import DataQualityChecker, get_rejection_reasons
from utils.logger import get_logger

logger = get_logger(__name__)

# Initialize Spark Session
spark_session = (
    SparkSession.builder.appName("CustomersRawToStructured")
    .enableHiveSupport()
    .getOrCreate()
)

# Configuration
RAW_TABLE = "raw.customers"
STRUCTURED_TABLE = "structured.customers"
REJECTED_TABLE = "structured.customers_rejected"
COUNTRY_CODES_PATH = "file:///opt/datasets/static_data/country_codes.csv"
DEPARTMENT_CODES_PATH = "file:///opt/datasets/static_data/department_codes.csv"

# Initialize readers and writers
delta_reader = DeltaTableReader(spark_session)
static_reader = StaticDataReader(spark_session)
quality_checker = DataQualityChecker(spark_session)

logger.info("========== Starting Customers Raw to Structured ETL ==========")

# Read raw data
logger.info("Reading data from raw.customers")
raw_df = delta_reader.read_delta_table(
    table_name=RAW_TABLE,
)

# Read static reference data for validation
logger.info("Reading static reference data")
country_codes_df = static_reader.read_static_csv(COUNTRY_CODES_PATH)
department_codes_df = static_reader.read_static_csv(DEPARTMENT_CODES_PATH)

# Broadcast small lookup tables for optimization
country_codes_df = country_codes_df.hint("broadcast")
department_codes_df = department_codes_df.hint("broadcast")

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
    required_columns=["customer_id", "email"],
    tag_column="has_null_required"
)

# 3. Check email format
logger.info("Step 3: Validating email format")
valid_df = quality_checker.check_email_format(
    valid_df, 
    email_column="email",
    valid_flag_column="is_valid_email"
)

# 4. Check referential integrity - Country Code
logger.info("Step 4: Checking country code validity")
valid_df = quality_checker.check_referential_integrity(
    valid_df,
    lookup_df=country_codes_df,
    check_column="country_code",
    lookup_column="country_code",
    flag_column="is_valid_country"
)

# 5. Check referential integrity - Department Code
logger.info("Step 5: Checking department code validity")
valid_df = quality_checker.check_referential_integrity(
    valid_df,
    lookup_df=department_codes_df,
    check_column="department_code",
    lookup_column="department_code",
    flag_column="is_valid_department"
)

# 6. Check for duplicates
logger.info("Step 6: Checking for duplicates")
valid_df, duplicate_df = quality_checker.check_duplicates(valid_df, key_columns=["customer_id"])

# =============================================================================
# SEPARATE VALID AND REJECTED RECORDS
# =============================================================================

logger.info("Separating valid and rejected records")

# Define rejection criteria
rejection_condition = (
    (col("has_null_required") == "Y") |
    (col("is_valid_email") == "N")
)

rejected_quality_df = valid_df.filter(rejection_condition)
clean_df = valid_df.filter(~rejection_condition)

# Add rejection reasons
rejection_checks = {
    "has_null_required": "Required field is null",
    "is_valid_email": "Invalid email format"
}
rejected_quality_df = get_rejection_reasons(rejected_quality_df, rejection_checks)

# =============================================================================
# TRANSFORM CLEAN DATA FOR STRUCTURED LAYER
# =============================================================================

logger.info("Transforming clean data for structured layer")
logger.info(f"columns={clean_df.columns}")
structured_df = clean_df.select(
    col("customer_id"),
    sha2(col("first_name"), 256).alias("first_name"),
    sha2(col("last_name"), 256).alias("last_name"),
    sha2(col("email"), 256).alias("email"),
    sha2(col("phone_number"), 256).alias("phone_number"),
    col("signup_date"),
    upper(trim(col("country_code"))).alias("country_code"),
    upper(trim(col("department_code"))).alias("department_code"),
    col("is_valid_country"),
    col("is_valid_department"),
    current_timestamp().alias("structured_insertion_timestamp")
)

# =============================================================================
# WRITE TO STRUCTURED LAYER (MERGE)
# =============================================================================

logger.info("Writing clean data to structured.customers using merge")

logger.info(f"Columns = {structured_df.columns}")

structured_writer = DeltaTableWriter(spark_session, STRUCTURED_TABLE)
structured_writer.write_merge(
    df=structured_df,
    merge_col="customer_id",
)

# =============================================================================
# WRITE REJECTED RECORDS
# =============================================================================

logger.info("Writing rejected records")

# Combine all rejected records
all_rejected_columns = [
    "customer_id", "first_name", "last_name", "email", "phone_number",
    "signup_date", "country_code", "department_code", "_corrupt_records",
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