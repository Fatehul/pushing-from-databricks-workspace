# Databricks notebook source
name="Sani"
department= "Data scientist"

# COMMAND ----------

print(f"I am {name} working as a {department}")

# COMMAND ----------



# COMMAND ----------

import urllib

# COMMAND ----------

access_key = ""
secret_key = ""
encoded_secret_key = secret_key.replace("/", "%2F")
aws_bucket_name = ""
mount_name = ""

dbutils.fs.mount(f"s3a://{access_key}:{encoded_secret_key}@{aws_bucket_name}", f"/mnt/{mount_name}")

# COMMAND ----------

access_key = "AKIAZLD3HSQXIGBPHC53"
secret_key = "lK/Z0tDXZelUCC9S+O8bHa9f7y3D0hTvHaFwf2KV"
encoded_secret_key = secret_key.replace("/", "%2F")
aws_bucket_name = "nlys3"
mount_name = "aws"

dbutils.fs.mount(f"s3a://{access_key}:{encoded_secret_key}@{aws_bucket_name}", f"/mnt/{mount_name}")

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/aws/raw/pit_stops.json

# COMMAND ----------

df=spark.read.json("dbfs:/mnt/aws/raw/pit_stops.json")

# COMMAND ----------

display(df)

# COMMAND ----------

df=spark.read.option("multiline",True).json("dbfs:/mnt/aws/raw/pit_stops.json")

# COMMAND ----------

Add 2 new columns to df. 
1. current timestamp 
2. path
and 
write the final df back to databrickshexaware adls account in your folder(parquet file)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df1=df.withColumn("current_date",current_timestamp())\
    .withColumn("path",input_file_name())

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/databrickshexaware/raw/output/

# COMMAND ----------

df1.write.mode("overwrite").format("parquet").save("dbfs:/mnt/databrickshexaware/raw/output/naval/pitstop")

# COMMAND ----------


