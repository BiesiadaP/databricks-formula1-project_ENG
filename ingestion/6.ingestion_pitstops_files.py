# Databricks notebook source
from pyspark.sql.types import  StructType, StructField, IntegerType, StringType, DoubleType, DateType, TimestampType

pitstops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                   StructField("driverId", IntegerType(), True),
                                   StructField("stop", IntegerType(), True),
                                   StructField("lap", IntegerType(), True),
                                   StructField("time", StringType(), True),
                                   StructField("duration", StringType(), True),
                                   StructField("milliseconds", IntegerType(), True)])

# COMMAND ----------

pitstops_df = spark.read \
    .schema(pitstops_schema) \
    .option('multiLine', True) \
    .json("/mnt/formula1dlpb/raw/pit_stops.json") 


# COMMAND ----------

from pyspark.sql.functions import current_timestamp

final_df = pitstops_df.withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumn("ingestion_date", current_timestamp())


# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/pitstops")
