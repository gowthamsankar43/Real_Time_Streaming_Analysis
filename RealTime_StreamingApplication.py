# Databricks notebook source

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC create catalog if not exists streamingdataprocess;
# MAGIC use catalog streamingdataprocess;
# MAGIC create schema if not exists bronze;
# MAGIC create schema if not exists silver;
# MAGIC create schema if not exists gold;

# COMMAND ----------

connectionString = ""
eventHubName = "generate_data "

ehConf = {
  'eventhubs.connectionString' : sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString),
  'eventhubs.eventHubName': eventHubName
}

# COMMAND ----------

df = spark.readStream \
    .format("eventhubs") \
    .options(**ehConf) \
    .load() \

# Displaying stream: Show the incoming streaming data for visualization and debugging purposes
df.display()

df.writeStream\
    .option("checkpointLocation", "/dbfs/tmp/checkpoints/streamingdataprocess/bronze")\
    .outputMode("append")\
    .format("delta")\
    .toTable("streamingdataprocess.bronze.tolldata")
     

# COMMAND ----------

schema = StructType([
    StructField("id", StringType(), True),
    StructField("vehicle_id", StringType(), True), 
    StructField("vehicle_type", StringType(), True), 
    StructField("timestamp", StringType(), True), 
    StructField("location", StringType(), True), 
    StructField("amount", StringType(), True)
    ])
