# Databricks notebook source
constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read.schema(constructors_schema).json("/mnt/formula1dlpb/raw/constructors.json")
#display(constructors_df)

# COMMAND ----------

constructers_dropped_df = constructors_df.drop("url")
#display(constructers_dropped_df)

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp
consuctors_final_df = constructers_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                             .withColumnRenamed("constructorRef", "constructor_ref") \
                                             .withColumn("ingestion_date", current_timestamp()) 
                                             
#display(consuctors_final_df)


# COMMAND ----------


consuctors_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors")
