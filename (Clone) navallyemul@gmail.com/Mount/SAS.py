# Databricks notebook source
https://learn.microsoft.com/en-us/azure/databricks/storage/azure-storage#access-azure-data-lake-storage-gen2-or-blob-storage-using-a-sas-token

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.<storage-account>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.<storage-account>.dfs.core.windows.net", dbutils.secrets.get(scope="<scope>", key="<sas-token-key>"))

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.databrickshexaware.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.databrickshexaware.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.databrickshexaware.dfs.core.windows.net", "sp=racwdlmeop&st=2023-08-12T06:11:00Z&se=2023-08-16T14:11:00Z&spr=https&sv=2022-11-02&sr=c&sig=UzbEo5hLFKKWAARqDMuVa0J3EphjDXtROOXTFmkS%2FXg%3D")

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/databrickshexaware/

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@databrickshexaware.dfs.core.windows.net/formula1"))

# COMMAND ----------

df=spark.read.json("abfss://raw@databrickshexaware.dfs.core.windows.net/formula1/constructors.json")

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df1=df.withColumn("current_date",current_timestamp())\
    .withColumn("path",input_file_name())\
        .drop("url")

# COMMAND ----------

df1.write.parquet("abfss://raw@databrickshexaware.dfs.core.windows.net/output/naval/constructor")

# COMMAND ----------


