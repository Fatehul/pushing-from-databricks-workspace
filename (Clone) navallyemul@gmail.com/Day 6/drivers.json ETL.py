# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/

# COMMAND ----------

df=spark.read.json("dbfs:/mnt/databrickshexaware/raw/formula1/drivers.json")

# COMMAND ----------

display(df)

# COMMAND ----------

users_schema= StructType([StructField("driverId",IntegerType()),
                          StructField("driverRef",StringType()),
                          StructField("number",IntegerType()),
                          StructField("code",StringType()),
                          StructField("name",MapType(StringType(),StringType())),
                          StructField("dob",StringType()),
                          StructField("nationality",StringType()),
                          StructField("url",StringType())
])

# COMMAND ----------

df=spark.read.schema(users_schema).json("dbfs:/mnt/databrickshexaware/raw/formula1/drivers.json")

# COMMAND ----------

display(df)

# COMMAND ----------

df1=df.withColumn("forename",col("name.forename"))\
    .withColumn("surname",col("name.surname"))\
        .drop("name")

# COMMAND ----------

display(df)

# COMMAND ----------



# COMMAND ----------

df1=df.withColumn("current_date",current_timestamp())\
            .withColumn("path",input_file_name())

# COMMAND ----------

display(df1)

# COMMAND ----------

write: 
File 
Table

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema formula1

# COMMAND ----------

df.write.mode("overwrite").parquet("dbfs:/mnt/databrickshexaware/raw/output/naval")

# COMMAND ----------

    df.write.mode("overwrite").format("parquet").save("dbfs:/mnt/databrickshexaware/raw/output/naval/driver/parquet")

# COMMAND ----------

df.write.mode("overwrite").save("dbfs:/mnt/databrickshexaware/raw/output/naval/driver/delta")

# COMMAND ----------

df.write.mode("overwrite").format("delta").save("path")

# COMMAND ----------

df.write.mode("overwrite").format("parquet").saveAsTable("formula1.drivers_par")

# COMMAND ----------

df.write.mode("overwrite").format("csv").saveAsTable("tablename")

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("formula1.drivers_delta")

# COMMAND ----------

df.write.mode("overwrite").option("path","dbfs:/mnt/databrickshexaware/raw/output/naval/driver/deltatable").saveAsTable("formula1.drivers_delta1")

# COMMAND ----------

df.write.mode("overwrite").format("delta").saveAsTable("formula1.drivers_delta1")

# COMMAND ----------

# MAGIC %fs ls dbfs:/user/hive/warehouse/formula1.db/

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.drivers_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.drivers_delta1

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended formula1.drivers_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended formula1.drivers_delta1

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table formula1.drivers_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table formula1.drivers_delta1

# COMMAND ----------

df1=df.withColumnRenamed("driverId", "driver ID")

# COMMAND ----------

display(df1)

# COMMAND ----------

option("delta.columnMapping.mode","name")

# COMMAND ----------

df1.write.mode("overwrite").format("delta").save("dbfs:/mnt/databrickshexaware/raw/output/naval/driver/test")

# COMMAND ----------

df.write.mode("overwrite").format("parquet").save("dbfs:/mnt/databrickshexaware/raw/output/naval/driver/parquet")
