# Databricks notebook source

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %%sql
# MAGIC %sql
# MAGIC create catalog if not exists streamingdataprocess;
# MAGIC use catalog streamingdataprocess;
# MAGIC create schema if not exists bronze;
# MAGIC create schema if not exists silver;
# MAGIC create schema if not exists gold;
