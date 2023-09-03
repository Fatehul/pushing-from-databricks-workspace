# Databricks notebook source
# MAGIC %md
# MAGIC #### Mounting it by access key

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://<container-name>@<storage-account-name>.blob.core.windows.net",
  mount_point = "/mnt/<mount-name>",
  extra_configs = {"fs.azure.account.key.<storage-account-name>.blob.core.windows.net":dbutils.secrets.get(scope = "<scope-name>", key = "<key-name>")})

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://project@nlyadls.blob.core.windows.net",
  mount_point = "/mnt/nlyadls/project",
  extra_configs = {"fs.azure.account.key.nlyadls.blob.core.windows.net":"bNxsKYluU/ajaU2B0kAXSdlbMfLxJzkPymcUxGSAFPNqwKClg3rcbnPTAttktsRbiD9QPg1VkeEF+AStJ/Yjtg=="})

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://process@nlyadls.blob.core.windows.net",
  mount_point = "/mnt/nlyadls/process",
  extra_configs = {"fs.azure.account.key.nlyadls.blob.core.windows.net":"bNxsKYluU/ajaU2B0kAXSdlbMfLxJzkPymcUxGSAFPNqwKClg3rcbnPTAttktsRbiD9QPg1VkeEF+AStJ/Yjtg=="})

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/nlyadls/project/raw/

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/nlyadls/process/

# COMMAND ----------

df=spark.read.parquet("dbfs:/mnt/nlyadls/project/bronze/14.8.2023.parquet")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC create database iot

# COMMAND ----------

# MAGIC %sql
# MAGIC use iot

# COMMAND ----------

input_file="dbfs:/mnt/nlyadls/project/raw/"

# COMMAND ----------

output_file="dbfs:/mnt/nlyadls/process/"

# COMMAND ----------

(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format","json")
 .load(f"{input_file}")
 .writeStream
 .option("checkpointlocation",f"{input_file}/logs/checkpoint")
 .option("path",f"{input_file}/bronze")
 .table("iot.bronze")
)

# COMMAND ----------

(spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format","parquet")
 .option("cloudFiles.schemaLocation",f"{output_file}/logs/schema")
 .load(f"{input_file}")
 .writeStream
 .option("checkpointlocation",f"{output_file}/logs/checkpoint")
 .option("path",f"{output_file}/bronze")
 .table("iot.bronze")
)

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from iot.bronze

# COMMAND ----------

df=spark.read.table("iot.bronze")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

help(hour)

# COMMAND ----------

df1=df.withColumn("hour",hour("timestamp")).select("temperature","timestamp","hour").sort("hour")

# COMMAND ----------

df2=df1.groupby("hour").agg(avg("temperature").alias("avg_temp"))

# COMMAND ----------

df2.write.saveAsTable("iot.silver2")

# COMMAND ----------

# MAGIC %sql
# MAGIC use iot;
# MAGIC show tables;

# COMMAND ----------

# MAGIC %sql
# MAGIC select temperature, timestamp, day(`timestamp`) from iot.bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC select hour(`timestamp`) as days, avg(temperature) as avg_temp from iot.bronze group by days

# COMMAND ----------

# MAGIC %sql
# MAGIC Create Table iot.silver as 
# MAGIC select hour(`timestamp`) as hours, avg(temperature) as avg_temp from iot.bronze group by hours

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table silver2

# COMMAND ----------


