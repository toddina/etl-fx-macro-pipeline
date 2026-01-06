import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

spark = SparkSession.builder.getOrCreate()

def load_raw_data (input_path: str, bronze_table: str, file_type: str, mode: str = "append"):
    if file_type == "json":
        raw_df = spark.read.option("multiline", "true").json(input_path)

    elif file_type == "parquet":
        raw_df = spark.read.parquet(input_path)
    
    else:
        raise Exception(f"Invalid file type: {file_type}")
    
    raw_df = (
        raw_df
        .withColumn("processed_at", lit(None).cast("timestamp"))
    )
    raw_df.write.format("delta").mode(mode).saveAsTable(bronze_table)