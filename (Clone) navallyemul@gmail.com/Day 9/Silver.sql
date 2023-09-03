-- Databricks notebook source
use development.jobs

-- COMMAND ----------

CREATE TABLE development.jobs.silver AS select day(timestamp) as days, temperature from jobs.bronze

-- COMMAND ----------


