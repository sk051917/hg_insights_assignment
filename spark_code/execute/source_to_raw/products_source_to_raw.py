from pyspark.sql.functions import current_timestamp
from adapters.data_reader import SourceDataReader
from adapters.data_writer import StreamDataWriter
from pyspark.sql import SparkSession
from config.schemas import PRODUCTS_INPUT_CSV_SCHEMA

spark_session = (
    SparkSession.builder.appName("MyApp")
    .enableHiveSupport()
    .getOrCreate()
)
# Use file:// prefix for local filesystem
csv_path = "file:///opt/datasets/products/"
checkpoint_path = "file:///opt/datasets/checkpoints/products/"
raw_table_name = "raw.products"

# Read data
print("========== Reading data from source ==========")
source_data_reader = SourceDataReader(spark_session=spark_session)
input_data_from_source = source_data_reader.read_data_as_stream(path=csv_path, schema=PRODUCTS_INPUT_CSV_SCHEMA)
input_data_from_source = input_data_from_source.withColumn(
    "insertion_timestamp", current_timestamp()
)

# Write data
print("========== Writing data to raw ==========")
raw_data_writer = StreamDataWriter(table_name=raw_table_name)
raw_data_writer.write_stream_data(
    dataset=input_data_from_source, checkpoint_path=checkpoint_path
)
