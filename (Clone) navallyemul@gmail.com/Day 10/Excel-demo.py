# Databricks notebook source
# MAGIC %run "./df"

# COMMAND ----------

input_file="abfss://raw@databrickshexaware.dfs.core.windows.net/metastore/input_stream/"

# COMMAND ----------

df=spark.read.format("csv").load("abfss://raw@databrickshexaware.dfs.core.windows.net/metastore/input_stream/demo.xlsx")

# COMMAND ----------

display(df)

# COMMAND ----------

df=spark.read.format("com.crealytics.spark.excel").load("abfss://raw@databrickshexaware.dfs.core.windows.net/metastore/input_stream/demo.xlsx")

# COMMAND ----------

df=spark.read.format("com.crealytics.spark.excel").option("header",True).load("abfss://raw@databrickshexaware.dfs.core.windows.net/metastore/input_stream/demo.xlsx")

# COMMAND ----------

display(df)

# COMMAND ----------


