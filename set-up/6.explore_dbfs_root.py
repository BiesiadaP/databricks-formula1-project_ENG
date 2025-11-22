# Databricks notebook source
# MAGIC %md
# MAGIC ## Explore DBFS root

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

display(dbutils.fs.ls('/FileStore/'))

# COMMAND ----------

display(spark.read.csv('dbfs:/FileStore/circuits.csv'))