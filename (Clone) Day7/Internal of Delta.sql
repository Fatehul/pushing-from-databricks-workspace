-- Databricks notebook source
show databases

-- COMMAND ----------

use naval

-- COMMAND ----------

show tables

-- COMMAND ----------

select * from people10m_external

-- COMMAND ----------

describe extended people10m_external

-- COMMAND ----------

describe history people10m_external

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Parquet Files + _delta_logs (Log Files)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC _delta_log
-- MAGIC 1. CRC
-- MAGIC 2. JSON transaction Log Files
-- MAGIC 3. Parquet Checkpoint 

-- COMMAND ----------

select * from people10m_external

-- COMMAND ----------

-- MAGIC %fs ls 

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/mnt/databrickshexaware/raw/delta/naval/people10m_external/_delta_log/

-- COMMAND ----------

-- MAGIC %fs head
-- MAGIC dbfs:/mnt/databrickshexaware/raw/delta/naval/people10m_external/_delta_log/00000000000000000000.json

-- COMMAND ----------

describe extended people10m_external

-- COMMAND ----------

INSERT INTO people10m_external VALUES(1,'a','b','z','M',2023-08-16,"123",5000);

-- COMMAND ----------

-- MAGIC %fs head
-- MAGIC dbfs:/mnt/databrickshexaware/raw/delta/naval/people10m_external/_delta_log/00000000000000000002.json

-- COMMAND ----------

select * from people10m_external

-- COMMAND ----------

INSERT INTO people10m_external VALUES(2,'c','b','z','F',2023-08-16,"123",7000);

-- COMMAND ----------

INSERT INTO people10m_external VALUES(3,'c','b','z','F',2023-08-16,"123",7000),
(4,'c','b','z','F',2023-08-16,"123",7000),
(5,'c','b','z','F',2023-08-16,"123",7000)


-- COMMAND ----------

select * from people10m_external

-- COMMAND ----------

describe history people10m_external

-- COMMAND ----------

delete from people10m_external where id=1

-- COMMAND ----------

select * from people10m_external

-- COMMAND ----------

-- MAGIC %fs head
-- MAGIC dbfs:/mnt/databrickshexaware/raw/delta/naval/people10m_external/_delta_log/00000000000000000005.json

-- COMMAND ----------

delete from people10m_external where id=3

-- COMMAND ----------

select * from people10m_external

-- COMMAND ----------

describe history people10m_external

-- COMMAND ----------

UPDATE people10m_external
SET salary= 10000
where id=2;

-- COMMAND ----------

select * from people10m_external

-- COMMAND ----------

describe history people10m_external

-- COMMAND ----------

-- MAGIC %fs head
-- MAGIC dbfs:/mnt/databrickshexaware/raw/delta/naval/people10m_external/_delta_log/00000000000000000007.json

-- COMMAND ----------

INSERT INTO people10m_external VALUES(6,'c','b','z','F',2023-08-16,"123",7000);
INSERT INTO people10m_external VALUES(7,'c','b','z','F',2023-08-16,"123",7000);

-- COMMAND ----------

describe history people10m_external

-- COMMAND ----------

INSERT INTO people10m_external VALUES(9,'c','b','z','F',2023-08-16,"123",7000);

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.read.parquet("dbfs:/mnt/databrickshexaware/raw/delta/naval/people10m_external/_delta_log/00000000000000000010.checkpoint.parquet").display()

-- COMMAND ----------

describe history people10m_external 

-- COMMAND ----------

select * from people10m_external version as of 2

-- COMMAND ----------

describe history people10m_external

-- COMMAND ----------

INSERT INTO people10m_external VALUES(15,'c','b','z','F',2023-08-16,"123",7000);

-- COMMAND ----------

describe history people10m_external

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Time Travel 
-- MAGIC 1. version as of 
-- MAGIC 2. timestamp

-- COMMAND ----------

select * from people10m_external timestamp as of '2023-08-16T05:01:17.000+0000'

-- COMMAND ----------

select * from people10m_external version as of 12

-- COMMAND ----------



-- COMMAND ----------

select * from people10m_external@v10 

-- COMMAND ----------

delete from people10m_external

-- COMMAND ----------

select * from people10m_external

-- COMMAND ----------

describe history people10m_external

-- COMMAND ----------

select * from people10m_external version as of 14

-- COMMAND ----------

restore table people10m_external to version as of 14

-- COMMAND ----------

select * from people10m_external

-- COMMAND ----------

optimize people10m_external

-- COMMAND ----------

select * from people10m_external where id= 5

-- COMMAND ----------

optimize people10m_external
zorder by (id)

-- COMMAND ----------

select * from people10m_external where id= 5

-- COMMAND ----------

describe history people10m_external

-- COMMAND ----------

use naval

-- COMMAND ----------

select * from people10m_external

-- COMMAND ----------

To remove unused files 
vacuum 
(because once you execute it you wont able to do time travel)

-- COMMAND ----------

vacuum people10m_external

-- COMMAND ----------

desc history people10m_external

-- COMMAND ----------

select * from people10m_external version as of 4

-- COMMAND ----------

Vacuum command the rentention period is of 7 day= 168 hours 

-- COMMAND ----------

vacuum people10m_external dry run 

-- COMMAND ----------

dry run
it will lsit down all the parquet that will be deleted after 7 days

-- COMMAND ----------

vacuum people10m_external retain 240 hours

-- COMMAND ----------

vacuum people10m_external retain 0 hours

-- COMMAND ----------

Set spark.databricks.delta.retentionDurationCheck.enabled = false

-- COMMAND ----------

vacuum people10m_external retain 0 hours dry run

-- COMMAND ----------

vacuum people10m_external retain 0 hours

-- COMMAND ----------


