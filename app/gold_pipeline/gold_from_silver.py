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
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get() # pyright: ignore[reportUndefinedVariable]
repo_root = f"/Workspace{notebook_path.split('/app')[0]}"
sys.path.append(f"{repo_root}/app/helpers")

from general_helpers import log # pyright: ignore

# DBTITLE 1,Initialize settings
# Source data path
silver_path = f"{repo_root}/data/silver/transactions" # should use this path for local running

# Target data path
gold_path = f"{repo_root}/data/gold/transactions" # should use this path for local running

# COMMAND ----------


