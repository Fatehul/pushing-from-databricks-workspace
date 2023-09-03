# Databricks notebook source
Ingesting the data into your lakehouse
1. Autoloader(Streaming)
2. COPY INTO (SQL)

# COMMAND ----------

# Syntax
spark.readStream
.format("cloudFiles")
.option("cloudFiles.format", <source_format>)
.load('/path/to/files’)
.writeStream
.option("checkpointLocation", <checkpoint_directory>)
.table(<table_name>)


# COMMAND ----------

input_file="dbfs:/mnt/databrickshexaware/raw/input_stream"

# COMMAND ----------

output_path="dbfs:/mnt/databrickshexaware/raw/output_stream"

# COMMAND ----------

(spark.readStream
.format("cloudFiles")
.option("cloudFiles.format", "csv")
.load(f"{input_file}")
.writeStream
.option("checkpointLocation", f"{output_path}/naval/autoloader/checkpoint")
.option("path",f"{output_path}/naval/autoloader/output")
.table("autoloader")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC use naval

# COMMAND ----------

( spark.readStream
.format("cloudFiles")
.option("cloudFiles.format", "csv")
.option("cloudFiles.schemaLocation",f"{output_path}/naval/autoloader/schemalocation")
.load(f"{input_file}")
.writeStream
.option("checkpointLocation", f"{output_path}/naval/autoloader/checkpoint")
.option("path",f"{output_path}/naval/autoloader/output")
.table("naval.autoloader")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.naval.autoloader

# COMMAND ----------

( spark.readStream
.format("cloudFiles")
.option("cloudFiles.format", "csv")
.option("cloudFiles.inferColumnTypes",True)
.option("cloudFiles.schemaLocation",f"{output_path}/naval/autoloader1/schemalocation")
.load(f"{input_file}")
.writeStream
.option("checkpointLocation", f"{output_path}/naval/autoloader1/checkpoint")
.option("path",f"{output_path}/naval/autoloader1/output")
.table("naval.autoloader1")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from naval.autoloader1

# COMMAND ----------

( spark.readStream
.format("cloudFiles")
.option("cloudFiles.format", "csv")
.option("cloudFiles.inferColumnTypes",True)
.option("cloudFiles.schemaLocation",f"{output_path}/naval/autoloader1/schemalocation")
.load(f"{input_file}")
.writeStream
.option("checkpointLocation", f"{output_path}/naval/autoloader1/checkpoint")
.option("path",f"{output_path}/naval/autoloader1/output")
.table("naval.autoloader1")
)

# COMMAND ----------

( spark.readStream
.format("cloudFiles")
.option("cloudFiles.format", "csv")
.option("cloudFiles.inferColumnTypes",True)
.option("cloudFiles.schemaLocation",f"{output_path}/naval/autoloader1/schemalocation")
.load(f"{input_file}")
.writeStream
.option("checkpointLocation", f"{output_path}/naval/autoloader1/checkpoint")
.option("path",f"{output_path}/naval/autoloader1/output")
.option("mergeSchema",True)
.table("naval.autoloader1")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from naval.autoloader1

# COMMAND ----------

( spark.readStream
.format("cloudFiles")
.option("cloudFiles.format", "csv")
.option("cloudFiles.inferColumnTypes",True)
.option("cloudFiles.schemaLocation",f"{output_path}/naval/autoloader1/schemalocation")
.load(f"{input_file}")
.writeStream
.option("checkpointLocation", f"{output_path}/naval/autoloader1/checkpoint")
.option("path",f"{output_path}/naval/autoloader1/output")
.option("mergeSchema",True)
.table("naval.autoloader1")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from naval.autoloader1

# COMMAND ----------

Stream should NOT STOP

# COMMAND ----------

( spark
.readStream
.format("cloudFiles")
.option("cloudFiles.format", "csv")
.option("cloudFiles.inferColumnTypes",True)
.option("cloudFiles.schemaEvolutionMode","rescue")
.option("cloudFiles.schemaLocation",f"{output_path}/naval/autoloader2/schemalocation")
.load(f"{input_file}")
.writeStream
.option("checkpointLocation", f"{output_path}/naval/autoloader2/checkpoint")
.option("path",f"{output_path}/naval/autoloader2/output")
.option("mergeSchema",True)
.table("naval.autoloader2")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from autoloader2

# COMMAND ----------



# COMMAND ----------


