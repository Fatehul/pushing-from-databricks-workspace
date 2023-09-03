# Databricks notebook source
# MAGIC %md
# MAGIC #### View (virtual table)
# MAGIC 1. Standard View
# MAGIC 2. Temp view
# MAGIC 3. global temp view

# COMMAND ----------

# MAGIC %python
# MAGIC CTAS

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.naval.autoloader

# COMMAND ----------

# MAGIC %sql
# MAGIC select id, name,gender,salary, country, date from naval.autoloader where country= 'India'

# COMMAND ----------

# MAGIC %sql
# MAGIC create view test_view_india as select id, name,gender,salary, country, date from naval.autoloader where country= 'India'

# COMMAND ----------

# MAGIC %sql
# MAGIC show views

# COMMAND ----------

# MAGIC %sql
# MAGIC create temp view test_view_usa as select id, name,gender,salary, country, date from naval.autoloader where country= 'USA'

# COMMAND ----------

# MAGIC %sql
# MAGIC show views

# COMMAND ----------

# MAGIC %python 
# MAGIC input_file="dbfs:/mnt/databrickshexaware/raw/input_practice/Baby_Names.csv"

# COMMAND ----------

df=spark.read.option("header",True).option("inferSchema",True).csv(f"{input_file}")

# COMMAND ----------

df.createOrReplaceTempView("temp_baby")

# COMMAND ----------

df.createOrReplaceGlobalTempView("global_temp_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC show views

# COMMAND ----------

# MAGIC %sql
# MAGIC show views in global_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.global_temp_view

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from temp_baby

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from temp_baby where Sex='F'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from temp_baby where year= 2007 

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE temp_baby SET TBLPROPERTIES (
# MAGIC    'delta.columnMapping.mode' = 'name')

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC Create table naval.babyname_2007 as select year, 'First Name' as firstname, county, Sex, count from temp_baby where year= 2007 

# COMMAND ----------

# MAGIC %sql 
# MAGIC use naval;
# MAGIC show tables;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended babyname_2007

# COMMAND ----------


