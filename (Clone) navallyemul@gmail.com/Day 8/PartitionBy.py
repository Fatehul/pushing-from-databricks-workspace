# Databricks notebook source
# MAGIC %fs ls dbfs:/mnt/databrickshexaware/raw/input_practice/

# COMMAND ----------

input_file="dbfs:/mnt/databrickshexaware/raw/input_practice/Baby_Names.csv"

# COMMAND ----------

df=spark.read.option("header",True).option("inferSchema",True).csv(f"{input_file}")

# COMMAND ----------

display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

df.groupBy("Year").count().sort("Year").display()

# COMMAND ----------

df.write.option("path","dbfs:/mnt/databrickshexaware/raw/output/naval/babynames/outputbyyear&gender1" )\
.partitionBy("Year","Sex")\
.saveAsTable("naval.babywithYear_Gender1")

# COMMAND ----------

df.write.option("path","dbfs:/mnt/databrickshexaware/raw/output/naval/babynames/output" )\
.option('delta.columnMapping.mode','name')\
.saveAsTable("naval.baby")

# COMMAND ----------



# COMMAND ----------

df.write.option("path","dbfs:/mnt/databrickshexaware/raw/output/naval/babynames/outputbyyear" )\
.option('delta.columnMapping.mode','name')\
.partitionBy("Year")\
.saveAsTable("naval.babywithYear")

# COMMAND ----------

df.write.option("path","dbfs:/mnt/databrickshexaware/raw/output/naval/babynames/outputbyyear&gender" )\
.option('delta.columnMapping.mode','name')\
.partitionBy("Year","Sex")\
.saveAsTable("naval.babywithYear_Gender")

# COMMAND ----------

df.write.option("path","dbfs:/mnt/databrickshexaware/raw/output/naval/babynames/outputbyyear&gender&parquet" )\
.format("parquet")\
.partitionBy("Year","Sex")\
.saveAsTable("naval.babywithYear_Gender_par")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.naval.babywithYear_Gender --where year=2008

# COMMAND ----------


