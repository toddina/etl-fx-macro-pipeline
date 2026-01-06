import sys
from delta.tables import DeltaTable
from pyspark.sql.functions import col

LOG_PATH = "/Workspace/Users/andreea.todosi@outlook.it/etl-fx-macro-pipeline/databricks_workspace/common"
sys.path.append(LOG_PATH)

from _logging import setup_job_logger

logger = setup_job_logger("silver_daily")

SRC_PATH = "/Workspace/Users/andreea.todosi@outlook.it/etl-fx-macro-pipeline/databricks_workspace/silver/src"
sys.path.append(SRC_PATH)

from transform_rates import transform_rates

BRONZE_TABLE = "bronze.forex_daily_data"
SILVER_TABLE = "silver.exchange_rates"

try:
    raw_df = spark.read.table(BRONZE_TABLE).filter(col("processed_at").isNull())

    updates_df = transform_rates(raw_df)

    silver_dt = DeltaTable.forName(spark, SILVER_TABLE)

    pk_cols = ['currency', 'exchange_date']

    pk_match_sql = " AND ".join([f"t.{col} = s.{col}" for col in pk_cols])

    update_set = {
        'rate': col('s.rate'),
        'processed_at': col('s.processed_at')
    }

    update_condition = 't.rate <> s.rate'

    (
        silver_dt.alias("t")
        .merge(
            source=updates_df.alias("s"), 
            condition=pk_match_sql
        )
        .whenMatchedUpdate(
            set=update_set,
            condition=update_condition
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    logger.info(f"Daily upsert completed successfully into {SILVER_TABLE}")

except Exception as e:
    logger.exception(f"Daily upsert failed: {e}")
    raise