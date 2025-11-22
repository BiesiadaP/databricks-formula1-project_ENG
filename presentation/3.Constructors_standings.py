# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

from pyspark.sql.functions import sum, count, when, col, desc, rank, dense_rank

constructors_standing_df = race_results_df \
    .groupBy("race_year", "team") \
    .agg(sum("points").alias("total_points"), count(when(col("position") == 1, True)).alias("wins"))
    
display(constructors_standing_df.filter("race_year = 2020").orderBy('wins', ascending=False))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

constructors_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
constructors_ranked_df = constructors_standing_df.withColumn("rank", rank().over(constructors_rank_spec))

display(constructors_ranked_df.filter("race_year = 2020"))


# COMMAND ----------

constructors_ranked_df.write.parquet(f"{presentation_folder_path}/constructors_standings", mode="overwrite")