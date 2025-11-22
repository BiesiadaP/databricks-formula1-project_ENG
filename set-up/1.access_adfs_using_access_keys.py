# Databricks notebook source
# MAGIC %md
# MAGIC # Access Azure Data Lake using accesss keys

# COMMAND ----------

formuladl_account_key = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dl-account-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1dlpb.dfs.core.windows.net",
    formuladl_account_key 
)

# COMMAND ----------

#display(dbutils.fs.ls("abfss://demo@formula1dlpb.dfs.core.windows.net"))

# COMMAND ----------

#display(spark.read.csv("abfss://demo@formula1dlpb.dfs.core.windows.net/circuits.csv"))