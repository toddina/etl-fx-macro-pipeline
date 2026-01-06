import sys
from pyspark.sql.functions import current_timestamp, lit, max, col

LOG_PATH = "/Workspace/Users/andreea.todosi@outlook.it/etl-fx-macro-pipeline/databricks_workspace/common"
sys.path.append(LOG_PATH)
from _logging import setup_job_logger

logger = setup_job_logger("gold_fx_macro_daily")

SRC_PATH = "/Workspace/Users/andreea.todosi@outlook.it/etl-fx-macro-pipeline/databricks_workspace/gold/src"
sys.path.append(SRC_PATH)

from build_macro_pivot import build_macro_pivot
from join_fx_with_macro import join_fx_with_macro

SILVER_FX_TABLE = "silver.exchange_rates"
SILVER_MACRO_TABLE = "silver.macro_finance"
GOLD_TABLE = "gold.fx_macro_daily"

try:
    if spark.catalog.tableExists(GOLD_TABLE):
        max_date = (
            spark.read.table(GOLD_TABLE)
            .agg(max("exchange_date"))
            .collect()[0][0]
        )
    else:
        max_date = None

    fx_df = spark.table(SILVER_FX_TABLE)

    if max_date:
        fx_df = fx_df.filter(col("exchange_date") > lit(max_date))
    
    if fx_df.isEmpty():
        logger.info("No new FX data to process")
        sys.exit(0)

    macro_df = spark.table(SILVER_MACRO_TABLE)

    if max_date:
        macro_df = macro_df.filter(col("market_date") > lit(max_date))   
        
    macro_pivot_df = build_macro_pivot(macro_df)
    gold_df = join_fx_with_macro(fx_df, macro_pivot_df)

    gold_df = gold_df.withColumn("processed_at", current_timestamp())
    
    if not spark.catalog.tableExists(GOLD_TABLE):
        (
            gold_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(GOLD_TABLE)
        )
        logger.info(f"Gold table {GOLD_TABLE} successfully built")

    else:
        gold_dt = DeltaTable.forName(spark, GOLD_TABLE)

        (
            gold_dt.alias("t")
            .merge(
                gold_df.alias("s"),
                "t.exchange_date = s.exchange_date AND t.currency = s.currency"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info(f"Gold table {GOLD_TABLE} successfully updated")

except Exception as e:
    logger.exception(f"GOLD job failed: {e}")
    raise