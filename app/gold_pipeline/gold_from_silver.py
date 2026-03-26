# Databricks notebook source
# DBTITLE 1,Initialize settings
# Imports
import os
import sys
import json
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, row_number, when, round, current_timestamp
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime

# Get Repo Root
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get() # pyright: ignore[reportUndefinedVariable]
repo_root = f"/Workspace{notebook_path.split('/app')[0]}"
sys.path.append(f"{repo_root}/app/helpers")

from general_helpers import log # pyright: ignore

# Source data path
silver_path = f"{repo_root}/data/silver/transactions" # should use this path for local running

# Target data path
gold_path = f"{repo_root}/data/gold/transactions" # should use this path for local running

# COMMAND ----------

# Read data
## TODO: to run locally should use silver_df = spark.read.format('delta').load(silver_path)
silver_df = spark.read.format('delta').table('silver.dbo.accounts_balance_cdi')

cdi_apply_df = silver_df.withColumn(
    "balance_to_apply_cdi",
    when(col("amount_cdi_not_applicable") < 0, col("amount_cdi_applicable") + col("amount_cdi_not_applicable"))
    .otherwise(col("amount_cdi_applicable"))
)

# Considering average cdi rate as 0,055% daily. This value should be retrieven from a table or an API
cdi_rate = 0.00055 
# Aplying and sum cdi bonus to current balance, using decimal (18,2) to deal with money and prevent any value mismatch
cdi_apply_df = cdi_apply_df.withColumn('cdi_bonus', (col('balance_to_apply_cdi') * cdi_rate).cast('decimal(18, 2)'))
cdi_apply_df = cdi_apply_df.withColumn('final_balance', (col('cdi_bonus') + col('total_amount')).cast('decimal(18, 2)'))

# COMMAND ----------

# DBTITLE 1,Cell 3
display(cdi_apply_df.limit(3))

# Now we need to create this tables:
# table with account, cdi_bonus and cdi_bonus_date, need to Federal Taxes calculations
# table with account, current_balance and last_day_bonus
 
# Table 1: account, cdi_bonus, cdi_bonus_date
cdi_bonus_df = cdi_apply_df.select(
    col("account_id"),
    col("cdi_bonus"),
    current_timestamp().alias("cdi_bonus_date")
)

# Table 2: account, current_balance, last_day_bonus
gold_account_balance_df = cdi_apply_df.select(
    col("account_id"),
    col("final_balance").alias("current_balance"),
    col("cdi_bonus").alias("last_day_bonus")
)

display(cdi_bonus_df.limit(3))
display(gold_account_balance_df.limit(3))

# COMMAND ----------

# Append cdi_apply_df to the current table to be possible audit the data
cdi_apply_df.write.format('delta').mode('append').option('mergeschema', 'true').saveAsTable('gold.dbo.daily_cdi_stagin_audit')

# Append cdi_bonus_df to the current table
cdi_bonus_df.write.format('delta').mode('append').option('mergeschema', 'true').saveAsTable('gold.dbo.daily_cdi_bonus')

# merge account_balance_df with the current table
try: 
    table_exists = spark.catalog.tableExists("gold.dbo.account_balance_last_day_cdi")
    if not table_exists:
        gold_account_balance_df.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable('gold.dbo.account_balance_last_day_cdi')
    else:
        target_table = DeltaTable.forName(spark, "silver.dbo.accounts_balance_cdi")
        
        target_table.alias("target").merge(
            source=gold_account_balance_df.alias("new_data"),
            condition= "target.account_id = new_data.account_id"
        ).whenNotMatchedInsertAll(
        ).whenMatchedUpdate(
            set = {
                "acount_id": "new_data.account_id",
                "current_balance": "new_data.current_balance",
                "last_day_bonus": "new_data.last_day_bonus"
            }
        ).execute()
except Exception as error:
    log(f"Error: {error}")
    raise error
