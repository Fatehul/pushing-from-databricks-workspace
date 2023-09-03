# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/databrickshexaware/raw/input_practice/

# COMMAND ----------

df=spark.read.option("multiline",True).json("dbfs:/mnt/databrickshexaware/raw/input_practice/complex.json")

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.withColumn("batters",explode("batters.batter"))\
    .withColumn("batters_id",col("batters.id"))\
        .withColumn("batters_type",col("batters.type"))\
            .drop("batters")
.display()

# COMMAND ----------



# COMMAND ----------

dffinal= (df.withColumn("batters",explode("batters.batter"))\
    .withColumn("batters_id",col("batters.id"))\
        .withColumn("batters_type",col("batters.type"))\
            .drop("batters")\
.withColumn("topping",explode("topping"))\
    .withColumn("topping_id",col("topping.id") )\
        .withColumn("topping_type",col("topping.type") )\
            .drop("topping"))

# COMMAND ----------

display(dffinal)

# COMMAND ----------

dffinal.write.mode("overwrite").option("path","dbfs:/mnt/databrickshexaware/raw/output/naval/complexjson").saveAsTable("test.complexjson")

# COMMAND ----------

# MAGIC %sql
# MAGIC select batters_id from hive_metastore.test.complexjson group by batters_id

# COMMAND ----------


