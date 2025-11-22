# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, TimestampType

lap_times_scheema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("lap", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True)])

# COMMAND ----------

lap_times_df = spark.read \
.schema(lap_times_scheema) \
.csv("/mnt/formula1dlpb/raw/lap_times/lap_times_split*")


# COMMAND ----------

from pyspark.sql.functions import current_timestamp

final_lap_times_df = lap_times_df.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumn("ingestion_date", current_timestamp())


# COMMAND ----------

final_lap_times_df.write.mode("overwrite").parquet(f"{processed_folder_path}/lap_times")
