from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, explode, array, struct, lit, row_number, current_timestamp
)

def transform_macro_data(raw_df: DataFrame) -> DataFrame:
    exploded_df = (
        raw_df
        .select(
            col("Date").alias("market_date"),
            col("ingested_at"),
            col("processed_at"),
            explode(
                array(
                    struct(lit("GLD").alias("asset"), col("GLD").alias("value")),
                    struct(lit("USO").alias("asset"), col("USO").alias("value")),
                    struct(lit("^STOXX50E").alias("asset"), col("^STOXX50E").alias("value")),
                    struct(lit("^TNX").alias("asset"), col("^TNX").alias("value"))
                )
            ).alias("kv")
        )
        .select(
            "market_date",
            col("kv.asset"),
            col("kv.value"),
            col("ingested_at"),
            col("processed_at")
        )
    )

    transformed_df = (
        exploded_df
        .withColumn("market_date", col("market_date").cast("date"))
        .withColumn("value", col("value").cast("double"))
        .withColumn("processed_at", current_timestamp())
    )
    
    return transformed_df


def deduplicate_macro_data(df: DataFrame) -> DataFrame:
    w = (
        Window
        .partitionBy("market_date", "asset")
        .orderBy(col("ingested_at").desc())
    )

    return (
        df
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .drop("rn")
    )