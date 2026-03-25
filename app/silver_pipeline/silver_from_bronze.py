# Databricks notebook source
# DBTITLE 1,Imports
# Imports
import os
import sys
import json
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, row_number
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from delta.tables import DeltaTable
from datetime import datetime, timedelta

# Get Repo Root
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get() # pyright: ignore[reportUndefinedVariable]
repo_root = f"/Workspace{notebook_path.split('/app')[0]}"
sys.path.append(f"{repo_root}/app/helpers")

from general_helpers import log # pyright: ignore[reportUndefinedVariable]

# COMMAND ----------

# DBTITLE 1,Initialize settings
# Source data path
bronze_path = f"{repo_root}/data/bronze/bronze_transactions" # should use this path for local running

# Target data path
checkpoint_path = f"{repo_root}/data/checkpoints/bronze_to_silver"
silver_path = f"{repo_root}/data/silver/transactions" # should use this path for local running

def ingest_to_silver(df_batch : DataFrame, batch_id : str)->None:
    # time_limit should be setted like this to be considered at last day midnight 
    # time_limit = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    time_limit = '2024-10-06T00:00:00.000' # 24 hours before max event_time rounding to midnight

    eligible_transactions_df = df_batch.withColumn(
        'cdi_applicable',
        (col('event_time') < lit(time_limit)).cast('boolean')
    )

    # Grouping by account_id and cdi_applicable and summing amount 
    cdi_applicable_df = eligible_transactions_df.groupBy("account_id", "cdi_applicable").agg({"amount": "sum"})
    
    # Pivoting table by column cdi_applicable
    pivoted_delta_df = (cdi_applicable_df
        .groupBy("account_id")
        .pivot("cdi_applicable", [True, False]) # Definir os valores ajuda na performance
        .agg(F.sum("sum(amount)"))
    )

    # Renaming and reordering columns
    final_delta_df = (pivoted_delta_df
        .withColumnRenamed("true", "amount_cdi_applicable")
        .withColumnRenamed("false", "amount_cdi_not_applicable")
        .withColumn("total_amount", 
                    F.col("amount_cdi_applicable") + F.col("amount_cdi_not_applicable"))
        .select("account_id", "total_amount", "amount_cdi_applicable", "amount_cdi_not_applicable")
    )

    try: 
        table_exists = spark.catalog.tableExists("silver.dbo.accounts_balance_cdi")
        if not table_exists:
            final_delta_df.write \
                .format("delta") \
                .mode("overwrite") \
                .saveAsTable('silver.dbo.accounts_balance_cdi')
        else:
            target_df = spark.read.format('delta').table('silver.dbo.accounts_balance_cdi') # TODO FILTER ONLY ACCOUNTS IN final_delta_df
            union_df = target_df.union(final_delta_df)
            
            # grouping both, staging and target tables with proper aliases
            union_df = union_df.groupBy("account_id").agg(
                F.sum("total_amount").alias("total_amount"),
                F.sum("amount_cdi_applicable").alias("amount_cdi_applicable"),
                F.sum("amount_cdi_not_applicable").alias("amount_cdi_not_applicable")
            )

            target_table = DeltaTable.forName(spark, "silver.dbo.accounts_balance_cdi")
            
            target_table.alias("target").merge(
                source=union_df.alias("new_data"),
                condition= "target.account_id = new_data.account_id"
            ).whenNotMatchedInsertAll(
            ).whenMatchedUpdate(
                set = {
                    "total_amount": "new_data.total_amount",
                    "amount_cdi_applicable": "new_data.amount_cdi_applicable",
                    "amount_cdi_not_applicable": "new_data.amount_cdi_not_applicable"
                }
            ).execute()

    except Exception as error:
        log(f"Error: {error}")
        raise error

# COMMAND ----------

# DBTITLE 1, create bronze table
try:
    # Read data
    ## TODO: to run locally should use bronze_stream_df = (spark.readStream.format("delta").load(bronze_path))
    bronze_df = spark.readStream \
        .format("delta") \
        .table("bronze.dbo.bronze_transactions")
    
    # Display streaming data with checkpoint location
    checkpoint_path = f"{repo_root}/data/checkpoints/bronze_display"
    
    stream_query = (
        bronze_df.writeStream \
        .format("delta") \
        .trigger(once=True) \
        .option("checkpointLocation", checkpoint_path) \
        .foreachBatch(
            lambda df, id: ingest_to_silver(
                df, id
                )
            )
        .start()
    )
        
        # .option("mergeSchema", "true") \
        # .toTable('silver.dbo.transactions') # .option("path", silver_path) # This option should be used for local running

    # TODO: This option should be used for local running
    # bronze_df.writeStream \
    #     .format("delta") \
    #     .option("checkpointLocation", checkpoint_path) \
    #     .option("mergeSchema", "true") \
    #     .option("path", silver_path) \
    #     .start()
    
    # display(spark.sql("SELECT * FROM bronze_preview"))

except Exception as error:
    log(f"Error: {error}")
    raise error
