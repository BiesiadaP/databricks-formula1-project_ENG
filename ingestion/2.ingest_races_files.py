# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
races_schema = StructType(fields = [StructField("raceId", IntegerType(), True),
                                    StructField("year", IntegerType(), True),
                                    StructField("round", IntegerType()  , True),
                                    StructField("circuitId", IntegerType(), True),
                                    StructField("name", StringType(), True),
                                    StructField("date", StringType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("url", StringType(), True)])


# COMMAND ----------

#races_df.printSchema()

# COMMAND ----------

races_df = spark.read.csv("/mnt/formula1dlpb/raw/races.csv", 
                             header = True, 
                             inferSchema = True,
                             schema = races_schema
                             )

# COMMAND ----------

from pyspark.sql.functions import col
races_selected_df = races_df.select(col("raceId"), col("year"), col("round"), col("circuitId"), col("name"), col("date"), col("time"))

# COMMAND ----------

from pyspark.sql.functions import concat_ws, to_timestamp, lit, current_timestamp

races_renamed_df = races_selected_df.withColumnRenamed("raceId", "race_id") \
                                   .withColumnRenamed("circuitId", "circuit_id") \
                                   .withColumnRenamed("year", "race_year") \
                                   .withColumnRenamed("date", "race_date") \
                                   .withColumnRenamed("time", "race_time")

races_with_timestamp_df = races_renamed_df.withColumn(
    "race_timestamp",
    to_timestamp(concat_ws(" ", "race_date", "race_time"), "yyyy-MM-dd HH:mm:ss")
).drop("race_date", "race_time")

races_final_df = races_with_timestamp_df.withColumn("env", lit("production")) \
                                        .withColumn("ingestion_date", current_timestamp())


# COMMAND ----------

races_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/races")
