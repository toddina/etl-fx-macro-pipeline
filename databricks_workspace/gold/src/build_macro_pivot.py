from pyspark.sql import DataFrame
from pyspark.sql.functions import col, first

ASSET_COLUMN_MAP = {
    "GLD": "gld",
    "USO": "uso",
    "^STOXX50E": "stoxx50e",
    "^TNX": "us_10y_yield"
}

def build_macro_pivot(macro_df: DataFrame) -> DataFrame:
    filtered_df = macro_df.filter(col("asset").isin(list(ASSET_COLUMN_MAP.keys())))

    pivot_df = (
        filtered_df
        .groupBy("market_date")
        .pivot("asset", list(ASSET_COLUMN_MAP.keys()))
        .agg(first("value"))
    )

    for asset, col_name in ASSET_COLUMN_MAP.items():
        pivot_df = pivot_df.withColumnRenamed(asset, col_name)

    return pivot_df