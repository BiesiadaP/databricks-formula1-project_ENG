# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")
drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers") \
    .withColumnRenamed("nationality", "driver_nationality") \
    .withColumnRenamed("name", "driver_name") \
    .withColumnRenamed("number", "driver_number")
constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors") \
    .withColumnRenamed("name", "team")
races_df = spark.read.parquet(f"{processed_folder_path}/races") \
    .withColumnRenamed("name", "race_name") \
    .withColumnRenamed("race_timestamp", "race_date")
results_df = spark.read.parquet(f"{processed_folder_path}/results") \
    .withColumnRenamed("time", "race_time")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

output_df = (
    results_df.join(races_df, results_df.race_id == races_df.race_id, "inner")
    .join(drivers_df, results_df.driver_id == drivers_df.driver_id, "inner")
    .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id, "inner")
    .join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner")
    .select(
        "race_year",
        "race_name",
        "race_date",
        "location",
        "driver_name",
        "driver_number",
        "driver_nationality",
        "team",
        "grid",
        "fastest_lap",
        "race_time",
        "points",
        "position"
    )
    .withColumn("created_date", current_timestamp())
)

# COMMAND ----------

#display(output_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy('points', ascending=False))

# COMMAND ----------

output_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")