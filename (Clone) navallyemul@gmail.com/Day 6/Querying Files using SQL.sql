-- Databricks notebook source
-- MAGIC %fs ls dbfs:/mnt/databrickshexaware/raw/formula1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Querying Files 
-- MAGIC SELECT * FROM file_format.`/path/to/file`

-- COMMAND ----------

select * from csv.`dbfs:/mnt/databrickshexaware/raw/formula1/circuits.csv`

-- COMMAND ----------

select * from json.`dbfs:/mnt/databrickshexaware/raw/formula1/constructors.json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC CTAS

-- COMMAND ----------

CREATE TABLE formula1.constructor_sql AS select * from json.`dbfs:/mnt/databrickshexaware/raw/formula1/constructors.json`

-- COMMAND ----------

CREATE TABLE formula1.constructor_sql AS select * from json.`dbfs:/mnt/databrickshexaware/raw/formula1/constructors.json`

-- COMMAND ----------

select * from formula1.constructor_sql1

-- COMMAND ----------

CREATE TABLE formula1.constructor_sql1 location 'dbfs:/mnt/databrickshexaware/raw/output/naval/constructor_sql' AS select * from json.`dbfs:/mnt/databrickshexaware/raw/formula1/constructors.json`

-- COMMAND ----------

select * from delta.`dbfs:/mnt/databrickshexaware/raw/output/naval/constructor_sql`

-- COMMAND ----------

Create table formula1.constructor_sql as select * from delta.`dbfs:/mnt/databrickshexaware/raw/output/naval/constructor_sql`

-- COMMAND ----------

select * from formula1.constructor_sql

-- COMMAND ----------

select *, current_timestamp() as current_date, input_file_name() as path from formula1.constructor_sql1

-- COMMAND ----------

describe extended formula1.constructor_sql1

-- COMMAND ----------

select * from formula1.constructor_sql

-- COMMAND ----------

describe extended formula1.constructor_sql

-- COMMAND ----------

select * from json.`dbfs:/mnt/databrickshexaware/raw/formula1/drivers.json`

-- COMMAND ----------


