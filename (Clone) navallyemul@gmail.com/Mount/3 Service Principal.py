# Databricks notebook source
container_name       = ""
storage_account_name = ""
client_id            = ""
tenant_id            = ""
client_secret        = ""

# COMMAND ----------

container_name       = "raw"
storage_account_name = "databrickshexaware"
client_id            = "f7c69be1-1d21-4604-8f71-a80f5fe01c11"
tenant_id            = "07b60660-baaa-4ef2-ae2d-b98dcd54f84c"
client_secret        = "c438Q~g~eABFdXJN468In2MZPIsuQAcQGZcIxaX~"

# COMMAND ----------

clientid
tenantid
clientsecret

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope="key")

# COMMAND ----------

dbutils.secrets.get(scope='key', key= "clientid")

# COMMAND ----------

container_name       = "raw"
storage_account_name = "databrickshexaware"
client_id            = dbutils.secrets.get(scope='key', key= "clientid")
tenant_id            = dbutils.secrets.get(scope='key', key= "tenantid")
client_secret        = dbutils.secrets.get(scope='key', key= "clientsecret")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_account_name}/{container_name}",
  extra_configs = configs)

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/

# COMMAND ----------


