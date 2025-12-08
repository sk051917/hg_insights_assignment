from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, 
    when, 
    lit, 
    length, 
    regexp_extract, 
    trim,
    upper,
    coalesce, 
    array, 
    array_remove, 
    concat_ws,
    broadcast
)

from utils.logger import get_logger
logger = get_logger(__name__)


class DataQualityChecker:
    
    def __init__(self, spark_session):
        self.spark_session = spark_session
    
    def check_corrupt_records(self, df: DataFrame, corrupt_column: str = "_corrupt_records") -> tuple:

        logger.info(f"Checking for corrupt records in column: {corrupt_column}")
        
        if corrupt_column not in df.columns:
            logger.info("No corrupt records column found. Returning all as valid.")
            return df, df.filter("1=0")
        
        corrupt_df = df.filter(col(corrupt_column).isNotNull())
        valid_df = df.filter(col(corrupt_column).isNull())
        
        corrupt_count = corrupt_df.count()
        valid_count = valid_df.count()
        
        logger.info(f"Corrupt records: {corrupt_count}")
        logger.info(f"Valid records: {valid_count}")
        
        return valid_df, corrupt_df
    
    def check_null_values(
        self, 
        df: DataFrame, 
        required_columns: list,
        tag_column: str = "has_null_required"
    ) -> DataFrame:
        logger.info(f"Checking null values for columns: {required_columns}")
        
        null_condition = None
        for col_name in required_columns:
            if col_name in df.columns:
                condition = col(col_name).isNull()
                null_condition = condition if null_condition is None else (null_condition | condition)
        
        if null_condition is None:
            return df.withColumn(tag_column, lit("N"))
        
        df = df.withColumn(
            tag_column,
            when(null_condition, lit("Y")).otherwise(lit("N"))
        )
        
        null_count = df.filter(col(tag_column) == "Y").count()
        logger.info(f"Records with null in required columns: {null_count}")
        
        return df
    
    def check_email_format(
        self, 
        df: DataFrame, 
        email_column: str,
        valid_flag_column: str = "is_valid_email"
    ) -> DataFrame:

        logger.info(f"Validating email format in column: {email_column}")
        
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        
        df = df.withColumn(
            valid_flag_column,
            when(
                regexp_extract(col(email_column), email_pattern, 0) != "",
                lit("Y")
            ).otherwise(lit("N"))
        )
        
        invalid_count = df.filter(col(valid_flag_column) == "N").count()
        logger.info(f"Records with invalid email: {invalid_count}")
        
        return df
    
    def check_positive_values(
        self,
        df: DataFrame,
        numeric_columns: list,
        flag_prefix: str = "is_valid_"
    ) -> DataFrame:
        logger.info(f"Checking positive values for columns: {numeric_columns}")
        
        for col_name in numeric_columns:
            if col_name in df.columns:
                flag_column = f"{flag_prefix}{col_name}"
                df = df.withColumn(
                    flag_column,
                    when(
                        (col(col_name).isNotNull()) & (col(col_name) > 0),
                        lit("Y")
                    ).otherwise(lit("N"))
                )
                
                invalid_count = df.filter(col(flag_column) == "N").count()
                logger.info(f"Records with invalid {col_name}: {invalid_count}")
        
        return df
    
    def check_date_range(
        self,
        df: DataFrame,
        date_column: str,
        min_date: str = None,
        max_date: str = None,
        flag_column: str = "is_valid_date"
    ) -> DataFrame:
        logger.info(f"Checking date range for column: {date_column}")
        
        conditions = [col(date_column).isNotNull()]
        
        if min_date:
            conditions.append(col(date_column) >= lit(min_date))
        if max_date:
            conditions.append(col(date_column) <= lit(max_date))
        
        combined_condition = conditions[0]
        for cond in conditions[1:]:
            combined_condition = combined_condition & cond
        
        df = df.withColumn(
            flag_column,
            when(combined_condition, lit("Y")).otherwise(lit("N"))
        )
        
        invalid_count = df.filter(col(flag_column) == "N").count()
        logger.info(f"Records with invalid {date_column}: {invalid_count}")
        
        return df
    
    def check_referential_integrity(
        self,
        df: DataFrame,
        lookup_df: DataFrame,
        check_column: str,
        lookup_column: str,
        flag_column: str = "is_valid_ref"
    ) -> DataFrame:
        logger.info(f"Checking referential integrity: {check_column} against {lookup_column}")
        
        lookup_values = lookup_df.select(col(lookup_column).alias("_lookup_value")).distinct()
        
        df_with_lookup = df.join(
            broadcast(lookup_values.withColumn("_exists", lit("Y"))),
            df[check_column] == lookup_values["_lookup_value"],
            "left"
        )

        df_with_lookup = df_with_lookup.withColumn(
            flag_column,
            coalesce(col("_exists"), lit("N"))
        )
        df_with_lookup = df_with_lookup.drop("_exists", "_lookup_value")
        
        invalid_count = df_with_lookup.filter(col(flag_column) == "N").count()
        logger.info(f"Records with invalid {check_column} reference: {invalid_count}")
        
        return df_with_lookup
        
    def check_string_length(
        self,
        df: DataFrame,
        string_columns: dict,
        flag_prefix: str = "is_valid_length_"
    ) -> DataFrame:

        logger.info(f"Checking string lengths for columns: {list(string_columns.keys())}")
        
        for col_name, (min_len, max_len) in string_columns.items():
            if col_name in df.columns:
                flag_column = f"{flag_prefix}{col_name}"
                
                conditions = []
                if min_len is not None:
                    conditions.append(length(col(col_name)) >= min_len)
                if max_len is not None:
                    conditions.append(length(col(col_name)) <= max_len)
                
                if conditions:
                    combined = conditions[0]
                    for cond in conditions[1:]:
                        combined = combined & cond
                    
                    df = df.withColumn(
                        flag_column,
                        when(combined, lit("Y")).otherwise(lit("N"))
                    )
        
        return df
    
    def check_duplicates(
        self,
        df: DataFrame,
        key_columns: list
    ) -> tuple:

        logger.info(f"Checking duplicates on columns: {key_columns}")
        
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number
        
        window_spec = Window.partitionBy(*key_columns).orderBy(*key_columns)
        
        df_with_row_num = df.withColumn("_row_num", row_number().over(window_spec))
        
        deduplicated_df = df_with_row_num.filter(col("_row_num") == 1).drop("_row_num")
        duplicate_df = df_with_row_num.filter(col("_row_num") > 1).drop("_row_num")
        
        duplicate_count = duplicate_df.count()
        logger.info(f"Duplicate records found: {duplicate_count}")
        
        return deduplicated_df, duplicate_df


def get_rejection_reasons(df: DataFrame, check_columns: dict) -> DataFrame:
    
    reasons = []
    for flag_col, reason in check_columns.items():
        if flag_col in df.columns:
            reasons.append(
                when(col(flag_col) == "N", lit(reason)).otherwise(lit(None))
            )
    
    if not reasons:
        return df.withColumn("rejection_reason", lit(None))
    
    df = df.withColumn(
        "rejection_reason",
        concat_ws("; ", array_remove(array(*reasons), None))
    )
    
    return df