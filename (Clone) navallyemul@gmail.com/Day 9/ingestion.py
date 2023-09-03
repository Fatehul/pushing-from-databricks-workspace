# Databricks notebook source
input_file="abfss://raw@databrickshexaware.dfs.core.windows.net/metastore/input_stream/"

# COMMAND ----------

output_path="abfss://raw@databrickshexaware.dfs.core.windows.net/metastore/output_stream/"

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists development.jobs

# COMMAND ----------

( spark.readStream
.format("cloudFiles")
.option("cloudFiles.format", "json")
.option("cloudFiles.inferColumnTypes",True)
.option("cloudFiles.schemaLocation",f"{output_path}/naval/logs/schema")
.load(f"{input_file}")
.writeStream
.option("checkpointLocation", f"{output_path}/naval/logs/checkpoint")
.option("path",f"{output_path}/naval/output")
.trigger(once=True)
.table("development.jobs.bronze")
)

# COMMAND ----------


