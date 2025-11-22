# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

from pyspark.sql.functions import sum, count, when, col, desc, rank, dense_rank

driver_standing_df = race_results_df \
    .groupBy("race_year", "driver_name", "driver_nationality", "team") \
    .agg(sum("points").alias("total_points"), count(when(col("position") == 1, True)).alias("wins"))
    
display(driver_standing_df.filter("race_year = 2020").orderBy('wins', ascending=False))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
driver_ranked_df = driver_standing_df.withColumn("rank", rank().over(driver_rank_spec))

display(driver_ranked_df.filter("race_year = 2020"))


# COMMAND ----------

driver_ranked_df.write.parquet(f"{presentation_folder_path}/driver_standings", mode="overwrite")