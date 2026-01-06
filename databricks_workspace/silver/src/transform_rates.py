from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    explode,
    from_json,
    to_json,
    current_timestamp,
    to_date,
    to_timestamp,
    when
)

def transform_rates(raw_df: DataFrame) -> DataFrame:
    map_schema = "map<string,double>"

    exploded_df = (
        raw_df
        .select(
            col("base").alias("base_currency"),
            col("date").alias("exchange_date"),
            explode(
                from_json(to_json(col("results")), map_schema)
            ).alias("currency", "rate"),
            col("ingested_at"),
            col("processed_at")
        )
    )

    transformed_df = (
        exploded_df
        .withColumn(
            "exchange_date",
            when(
                col("exchange_date").rlike("T"),
                to_date(
                    to_timestamp(col("exchange_date"),"yyyy-MM-dd'T'HH:mm:ssX")
                )
            ).otherwise(
                to_date(col("exchange_date"))
            )
        )
        .withColumn("rate", col("rate").cast("double"))
        .withColumn("processed_at", current_timestamp())
    )

    return transformed_df






