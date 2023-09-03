-- Databricks notebook source
jdbcUsername = " "
jdbcPassword = " "
jdbcHostname = " 
jdbcPort = 1433
jdbcDatabase = " "
jdbcDriver= "com.microsoft.sqlserver.jdbc.SQLServerDriver"

jdbcUrl=f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};databaseName={jdbcDatabase};user={jdbcUsername};password={jdbcPassword}"

-- COMMAND ----------

jdbcUsername = "dataeng"
jdbcPassword = "test@123"
jdbcHostname = "sqldatabricks1"
jdbcPort = 1433
jdbcDatabase = "sqldatabricks"
jdbcDriver= "com.microsoft.sqlserver.jdbc.SQLServerDriver"

jdbcUrl=f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};databaseName={jdbcDatabase};user={jdbcUsername};password={jdbcPassword}"
