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
    StructField("entryTime", TimestampType(), True),
    StructField("carModel", StructType([
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("vehicleType", StringType(), True),
        StructField("vehicleWeight", IntegerType(), True),
    ]), True), 
    StructField("state", StringType(), True), 
    StructField("tollAmount", FloatType(), True), 
    StructField("tollId", IntegerType(), True), 
    StructField("licensePlate", StringType(), True)
    ])

# COMMAND ----------

df2 = spark.readStream \
    .format("delta")\
    .table("bronze.tolldata") \
    .withColumn('body', col('body').cast(StringType())) \
    .withColumn('body', from_json(col('body'), schema)) \
    .select("body.entryTime", "body.carModel.make", "body.carModel.model", "body.carModel.vehicleType", "body.carModel.vehicleWeight", "body.state", "body.tollAmount", "body.tollId", "body.licensePlate")

df2.display()

df2.writeStream \
    .option("checkpointLocation", "/dbfs/tmp/checkpoints/streamingdataprocess/silver") \
    .outputMode("append") \
    .format("delta") \
    .toTable("streamingdataprocess.silver.processeddata")


# COMMAND ----------

from pyspark.sql.functions import sum, count

df = spark.readStream \
    .format("delta") \
    .table("silver.processeddata") \
    .withWatermark("entryTime", "10 minutes") \
    .groupBy(window("entryTime", "10 minutes")) \
    .agg(
        avg("tollAmount").alias("AvgTollAmount"),
        count("licensePlate").alias("CountOfVehicles")
    ) \
    .select("window.start","window.end", "AvgTollAmount", "CountOfVehicles") \

display(df)

df.writeStream \
    .option("checkpointLocation", "/dbfs/tmp/checkpoints/streamingdataprocess/gold") \
    .outputMode("complete") \
    .format("delta") \
    .toTable("streamingdataprocess.gold.tollsummarydata")
