from pyspark.sql import DataFrame
from delta.tables import DeltaTable
from utils.logger import get_logger
from pyspark.sql.functions import lit, current_timestamp

logger = get_logger(__name__)


class StreamDataWriter:
    def __init__(self, table_name: str):
        self.table_name = table_name

    def write_stream_data(self, dataset: str, checkpoint_path: str) -> None:

        query = (
            dataset.writeStream.format("delta")
            .outputMode("append")
            .option("mergeSchema", "True")
            .option("checkpointLocation", checkpoint_path)
            .trigger(availableNow=True)
            .toTable(self.table_name)
        )

        query.awaitTermination()


class DeltaTableWriter:
    """Writer class for Delta tables with merge support."""
    
    def __init__(self, spark_session, table_name: str):
        self.spark_session = spark_session
        self.table_name = table_name

    def write_append(self, df: DataFrame, partition_columns: list = None) -> None:

        logger.info(f"Appending data to table: {self.table_name}")
        
        writer = df.write.format("delta").mode("append").option("mergeSchema", "True")
        
        if partition_columns:
            writer = writer.partitionBy(*partition_columns)
        
        writer.saveAsTable(self.table_name)
        
        logger.info(f"Successfully appended {df.count()} records to {self.table_name}")

    def write_merge(
        self,
        df: DataFrame,
        merge_col: str,
    ) -> None:

        logger.info(f"Merging data into table: {self.table_name}")
        
        source_count = df.count()
        logger.info(f"Source records to merge: {source_count}")

        target_table = DeltaTable.forName(self.spark_session, self.table_name)
        
        merge_cond = f"source.{merge_col} = target.{merge_col}"
        logger.info(f"merge_cond: {merge_cond}")

        merge_builder = (
            target_table.alias("target")
            .merge(df.alias("source"), merge_cond)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
        )
        
        merge_builder.execute()
        
        logger.info(f"Merge completed successfully for {self.table_name}")


    def write_rejected_records(
        self,
        df: DataFrame,
        rejection_reason: str,
    ) -> None:
        
        logger.info(f"Writing rejected records to: {self.table_name}")
        logger.info(f"Rejection reason: {rejection_reason}")
        
        rejected_df = (
            df
            .withColumn("rejection_reason", lit(rejection_reason))
            .withColumn("rejected_at", current_timestamp())
        )
        
        self.write_append(rejected_df)
        
        logger.info(f"Wrote {df.count()} rejected records to {self.table_name}")