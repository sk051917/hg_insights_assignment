from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from utils.logger import get_logger

logger = get_logger(__name__)


class SourceDataReader:
    def __init__(self, spark_session: SparkSession):
        self.spark_session = spark_session

    def read_data_as_stream(
        self,
        path: str,
        schema: StructType,
        sep: str = ",",
    ) -> DataFrame:

        stream_data = (
            self.spark_session.readStream.format("csv")
            .option("header", True)
            .option("delimiter", sep)
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_records")
            .schema(schema)
            .load(path)
        )

        return stream_data


class DeltaTableReader:
    """Reader class for Delta tables."""
    
    def __init__(self, spark_session: SparkSession):
        self.spark_session = spark_session

    def read_delta_table(
        self,
        table_name: str,
        columns: list = None,
        filter_condition: str = None
    ) -> DataFrame:
        """
        Read data from a Delta table with optional column pruning and filtering.
        
        Args:
            table_name: Full table name (e.g., 'raw.customers')
            columns: List of columns to select (column pruning optimization)
            filter_condition: SQL filter condition string (predicate pushdown)
        
        Returns:
            DataFrame with the requested data
        """
        logger.info(f"Reading Delta table: {table_name}")
        
        df = self.spark_session.table(table_name)
        
        if filter_condition:
            logger.info(f"Applying filter: {filter_condition}")
            df = df.filter(filter_condition)
        
        if columns:
            logger.info(f"Selecting columns: {columns}")
            df = df.select(*columns)
        
        record_count = df.count()
        logger.info(f"Read {record_count} records from {table_name}")
        
        return df

    def read_delta_table_incremental(
        self,
        table_name: str,
        watermark_column: str,
        last_watermark_value: str = None,
        columns: list = None
    ) -> DataFrame:
        """
        Read data incrementally from a Delta table based on a watermark column.
        
        Args:
            table_name: Full table name
            watermark_column: Column to use for incremental reads
            last_watermark_value: Last processed watermark value
            columns: List of columns to select
        
        Returns:
            DataFrame with incremental data
        """
        logger.info(f"Reading incremental data from {table_name}")
        
        df = self.spark_session.table(table_name)
        
        if last_watermark_value:
            logger.info(f"Filtering where {watermark_column} > '{last_watermark_value}'")
            df = df.filter(f"{watermark_column} > '{last_watermark_value}'")
        
        if columns:
            df = df.select(*columns)
        
        record_count = df.count()
        logger.info(f"Read {record_count} incremental records from {table_name}")
        
        return df


class StaticDataReader:
    """Reader class for static/reference data CSV files."""
    
    def __init__(self, spark_session: SparkSession):
        self.spark_session = spark_session

    def read_static_csv(
        self,
        path: str,
        schema: StructType = None,
        sep: str = ","
    ) -> DataFrame:
        """
        Read static reference data from CSV files.
        
        Args:
            path: Path to the CSV file
            schema: Optional schema for the CSV
            sep: Delimiter character
        
        Returns:
            DataFrame with static data
        """
        logger.info(f"Reading static CSV from: {path}")
        
        reader = (
            self.spark_session.read.format("csv")
            .option("header", True)
            .option("delimiter", sep)
        )
        
        if schema:
            reader = reader.schema(schema)
        else:
            reader = reader.option("inferSchema", True)
        
        df = reader.load(path)
        
        record_count = df.count()
        logger.info(f"Read {record_count} records from static CSV")
        
        return df