# Databricks notebook source
1.Notebook 
- Access Key
- Shared Access Signuarture
- Service Principal

2. Cluster 

# COMMAND ----------

#dbutils.fs.help()

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://<container-name>@<storage-account-name>.blob.core.windows.net",
  mount_point = "/mnt/<mount-name>",
  extra_configs = {"fs.azure.account.key.<storage-account-name>.blob.core.windows.net":dbutils.secrets.get(scope = "<scope-name>", key = "<key-name>")})

# COMMAND ----------

storage-account-name= gayathri1adls
container_name= raw 

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://raw@gayathri1adls.blob.core.windows.net",
  mount_point = "/mnt/gayathri1adls/raw",
  extra_configs = {"fs.azure.account.key.gayathri1adls.blob.core.windows.net":"bAU6HsqWXLGa1of2A9AJqBghXhOzuXjpxmS0drydQTAJsBUS/1hHauNOUhdDWYZT1QE59px1IPH6+AStsnRrcg=="})

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://raw@databrickshexaware.blob.core.windows.net",
  mount_point = "/mnt/databrickshexaware/raw",
  extra_configs = {"fs.azure.account.key.databrickshexaware.blob.core.windows.net":"RDurKp8QLsrg0N7q+4MfL9M/2HGJO2eU2ZjVTMHG6+Jcs8WdUkSvzEgKwhuLLXi6QzaGamquNmZU+AStSnib4g=="})

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/databrickshexaware/raw/

# COMMAND ----------

#dbutils.fs.rm("%fs ls dbfs:/mnt/databrickshexaware/raw/private")

# COMMAND ----------

df=spark.read.option("header",True).option("inferschema",True).csv("dbfs:/mnt/gayathri1adls/raw/input/formula1/circuits.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df1= df.withColumn("current_time",current_timestamp())\
.withColumn("path",input_file_name())\
.drop("url")

# COMMAND ----------

df1.display()

# COMMAND ----------

df2=df1.withColumn("naem",lit("formula1_circuits"))

# COMMAND ----------

df2.display()

# COMMAND ----------

df2.write.mode("overwrite").parquet("dbfs:/mnt/databrickshexaware/raw/output/naval/circuit")

# COMMAND ----------

check


# COMMAND ----------

df=spark.read.parquet("dbfs:/mnt/databrickshexaware/raw/output/naval/circuit")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Unmount

# COMMAND ----------

dbutils.fs.unmount("/mnt/databrickshexaware/raw")

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/databrickshexaware/raw

# COMMAND ----------


