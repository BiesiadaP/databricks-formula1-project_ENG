# Databricks notebook source
# MAGIC %md
# MAGIC # Access Azure Data Lake using Service Principal

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula-client-id')
tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula-tenant-id')
client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula-client-secret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dlpb.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dlpb.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dlpb.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dlpb.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dlpb.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlpb.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlpb.dfs.core.windows.net/circuits.csv"))