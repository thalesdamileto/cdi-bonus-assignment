# Databricks notebook source
# DBTITLE 1,Imports
# Imports
import os
import sys
import json
from pyspark.sql.functions import col, lit, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime

# Get Repo Root
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = f"/Workspace{notebook_path.split('/pipelines_notebooks_templates')[0]}"
sys.path.append(f"{repo_root}/app")

from helpers import log

# COMMAND ----------

# DBTITLE 1, Initialize parameters
# Source data path
bronze_path = f"{repo_root}/data/bronze/bronze_transactions"

# Target data path
silver_path = f"{repo_root}/data/silver/transactions"

# COMMAND ----------

# DBTITLE 1, create bronze table

try:
    # Read data
    bronze_df = spark.read.format("parquet").load(bronze_path)
    bronze_df.show(5, truncate=False)
    display(bronze_df.limit(5))

except Exception as error:
    log(f"Error: {error}")
    raise error
