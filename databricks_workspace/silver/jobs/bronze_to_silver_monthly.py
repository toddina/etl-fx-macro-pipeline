import sys
from delta.tables import DeltaTable
from pyspark.sql.functions import col

LOG_PATH = "/Workspace/Users/andreea.todosi@outlook.it/etl-fx-macro-pipeline/databricks_workspace/common"
sys.path.append(LOG_PATH)

from _logging import setup_job_logger

logger = setup_job_logger("silver_monthly")

SRC_PATH = "/Workspace/Users/andreea.todosi@outlook.it/etl-fx-macro-pipeline/databricks_workspace/silver/src"
sys.path.append(SRC_PATH)

from transform_macro_data import (
    transform_macro_data,
    deduplicate_macro_data
)

BRONZE_TABLE = "bronze.macro_finance_monthly_data"
SILVER_TABLE = "silver.macro_finance"

try:
    raw_df = spark.read.table(BRONZE_TABLE).filter(col("processed_at").isNull())

    transformed_df = transform_macro_data(raw_df)
    deduped_df = deduplicate_macro_data(transformed_df)

    if not spark.catalog.tableExists(SILVER_TABLE):
        (
            deduped_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(SILVER_TABLE)
        )
        logger.info(f"Silver macro finance table {SILVER_TABLE} created")
    else:
        silver_dt = DeltaTable.forName(spark, SILVER_TABLE)
        
        pk_cols = ['market_date', 'asset']

        pk_match_sql = " AND ".join([f"t.{col} = s.{col}" for col in pk_cols])

        (
            silver_dt.alias("t")
            .merge(
                source=deduped_df.alias("s"),
                condition=pk_match_sql
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info(f"Monthly merge completed into {SILVER_TABLE}")

except Exception as e:
    logger.exception(f"Monthly merge failed: {e}")
    raise
                     



