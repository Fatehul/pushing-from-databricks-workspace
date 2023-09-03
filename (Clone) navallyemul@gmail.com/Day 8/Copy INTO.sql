-- Databricks notebook source
COPY INTO my_table
FROM '/path/to/files’
FILEFORMAT = <format>
FORMAT_OPTIONS (<format options>)
COPY_OPTIONS (<copy options>)

-- COMMAND ----------

CREATE TABLE naval.my_table

-- COMMAND ----------

COPY INTO naval.my_table
FROM 'dbfs:/mnt/databrickshexaware/raw/input_stream/'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header'='true')
COPY_OPTIONS ('mergeSchema'='True');

-- COMMAND ----------

select * from naval.my_table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC input_file2="dbfs:/mnt/databrickshexaware/raw/input_practice/Baby_Names.csv"
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.read.option("header",True).option("inferSchema",True).csv(f"{input_file2}")

-- COMMAND ----------


