from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import col, last

def join_fx_with_macro(fx_df: DataFrame, macro_df: DataFrame) -> DataFrame:
    macro_df = macro_df.withColumnRenamed(
        "market_date", "exchange_date"
    )

    joined_df = (
        fx_df.alias("fx")
        .join(
            macro_df.alias("m"),
            on="exchange_date",
            how="left"
        )
    )

    macro_cols = [
        "gld",
        "uso",
        "stoxx50e",
        "us_10y_yield"
    ]

    w = (
        Window
        .partitionBy("currency")
        .orderBy("exchange_date")
        .rowsBetween(Window.unboundedPreceding, 0)
    )

    for col in macro_cols:
        joined_df = joined_df.withColumn(
            col,
            last(col, ignorenulls=True).over(w)
        )

    return joined_df