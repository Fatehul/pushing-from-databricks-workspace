# Databricks notebook source
Batch 
read
df=spark.read.csv("path")
write
df.write.format("parquet").save("path")


Streaming
read
df=spark.readStream.csv("path")
write
df.writeStream.format("parquet").option("path","location").option("checkpointlocation","location").start()

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/databrickshexaware/raw/input_stream

# COMMAND ----------

df=spark.readStream.csv("dbfs:/mnt/databrickshexaware/raw/input_stream/Jan.CSV")

# COMMAND ----------

user_schema= "Id int, Name string ,Gender string ,Salary int ,Country string ,Date string"

# COMMAND ----------

Bronze- silver- gold.

# COMMAND ----------

df=spark.readStream.schema(user_schema).option("header",True).csv("dbfs:/mnt/databrickshexaware/raw/input_stream/")

# COMMAND ----------

display(df)

# COMMAND ----------

path="dbfs:/mnt/databrickshexaware/raw/output_stream"

# COMMAND ----------

df.writeStream\
.option("path",f"{path}/naval/stream/output")\
.option("checkpointlocation",f"{path}/naval/stream/checkpoint")\
.format("parquet")\
.start()

# COMMAND ----------

df=spark.read.parquet(f"{path}/naval/stream/output")

# COMMAND ----------

display(df)

# COMMAND ----------

for stream in spark.streams.active:
    stream.stop()

# COMMAND ----------


