# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, TimestampType

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True)])

# COMMAND ----------

qualifying_df = spark.read.schema(qualifying_schema) \
    .option("multiLine", True) \
    .json(f"{raw_folder_path}/qualifying/*")


# COMMAND ----------

from pyspark.sql.functions import current_timestamp
final_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
                       .withColumnRenamed("raceId", "race_id") \
                       .withColumnRenamed("driverId", "driver_id") \
                       .withColumnRenamed("constructorId", "constructor_id") \
                       .withColumn("ingestion_date", current_timestamp()) 

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/qualifying")
