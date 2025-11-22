# Databricks notebook source
# MAGIC %md
# MAGIC # Access Azure Data Lake using Service Principal

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula-client-id')
tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula-tenant-id')
client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula-client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula1dlpb.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dlpb/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dlpb/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dlpb/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mount other containers

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
  client_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula-client-id')
  tenant_id = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula-tenant-id')
  client_secret = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula-client-secret')

  configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
  
  dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)
  
  display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls('formula1dlpb', 'presentation')