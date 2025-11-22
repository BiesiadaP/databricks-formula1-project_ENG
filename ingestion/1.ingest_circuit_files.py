# Databricks notebook source
# MAGIC %md
# MAGIC ##ingest circuits.csv

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

#display(dbutils.fs.ls("/mnt/formula1dlpb/raw"))

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
circuits_schema = StructType(fields = [StructField("circuitId", IntegerType(), False),
                                      StructField("circuitRef", StringType(), True),
                                      StructField("name", StringType(), True),
                                      StructField("location", StringType(), True),
                                      StructField("country", StringType(), True),
                                      StructField("lat", DoubleType(), True),
                                      StructField("lng", DoubleType(), True),
                                      StructField("alt", IntegerType(), True),
                                      StructField("url", StringType(), True)])

# COMMAND ----------

circuits_df = spark.read.csv("/mnt/formula1dlpb/raw/circuits.csv", 
                             header = True, 
                             inferSchema = True,
                             schema = circuits_schema)

# COMMAND ----------

#circuits_df.printSchema()

# COMMAND ----------

#circuits_df.describe().show()

# COMMAND ----------

from pyspark.sql.functions import col
circuits_selected_df = circuits_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId","circuit_id") \
                                             .withColumnRenamed("circuitRef","circuit_ref") \
                                             .withColumnRenamed("lat","latitude") \
                                             .withColumnRenamed("lng","longitude") \
                                             .withColumnRenamed("alt","altitude")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp()) \
    .withColumn("env", lit("Production"))

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

#display(spark.read.parquet("/mnt/formula1dlpb/processed/circuits").limit(10))
