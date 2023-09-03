-- Databricks notebook source
create table development.naval.emp (id int, name varchar(20))

-- COMMAND ----------

select * from development.naval.emp

-- COMMAND ----------

insert into development.naval.emp values (1,'a')

-- COMMAND ----------

create catalog testing

-- COMMAND ----------

create table testing.default.emp (id int primary key, name varchar(20))

-- COMMAND ----------


