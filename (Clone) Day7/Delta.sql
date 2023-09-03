-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### How do I create a Delta table?
-- MAGIC - DataFrame 
-- MAGIC - SQL 
-- MAGIC - Python

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #df.write.format("delta").saveAsTable("table_name")

-- COMMAND ----------

create database naval

-- COMMAND ----------

use naval

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS naval.people10m (
  id INT,
  firstName STRING,
  middleName STRING,
  lastName STRING,
  gender STRING,
  birthDate TIMESTAMP,
  ssn STRING,
  salary INT
)

-- COMMAND ----------

desc extended naval.people10m

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/naval.db/people10m

-- COMMAND ----------

select * from people10m

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/mnt/databrickshexaware/raw/delta/naval

-- COMMAND ----------

CREATE OR REPLACE TABLE naval.people10m_external (
  id INT,
  firstName STRING,
  middleName STRING,
  lastName STRING,
  gender STRING,
  birthDate TIMESTAMP,
  ssn STRING,
  salary INT
)
location 'dbfs:/mnt/databrickshexaware/raw/delta/naval/people10m_external'

-- COMMAND ----------

select * from naval.people10m_external;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from delta.tables import *

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DeltaTable.createIfNotExists(spark) \
-- MAGIC   .tableName("default.people10m") \
-- MAGIC   .addColumn("id", "INT") \
-- MAGIC   .addColumn("firstName", "STRING") \
-- MAGIC   .addColumn("middleName", "STRING") \
-- MAGIC   .addColumn("lastName", "STRING", comment = "surname") \
-- MAGIC   .addColumn("gender", "STRING") \
-- MAGIC   .addColumn("birthDate", "TIMESTAMP") \
-- MAGIC   .addColumn("ssn", "STRING") \
-- MAGIC   .addColumn("salary", "INT") \
-- MAGIC   .execute()
-- MAGIC

-- COMMAND ----------

select * from default.people10m

-- COMMAND ----------

desc extended default.people10m

-- COMMAND ----------


