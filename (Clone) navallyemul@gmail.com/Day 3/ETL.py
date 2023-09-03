# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/tables/raw/emp.csv

# COMMAND ----------

path="dbfs:/FileStore/tables/raw/emp.csv"

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

users_schema= StructType([StructField("CustomerID",IntegerType()),
                          StructField("Name",StringType()),
                          StructField("Age",IntegerType()),
                          StructField("Gender",StringType()),
                          StructField("TotalSpend",IntegerType())
])

# COMMAND ----------

df=spark.read.option("header",True).schema(users_schema).csv(path)

# COMMAND ----------

display(df)

# COMMAND ----------

## 
1. Get New column= current_timestamp
2. rename column name from customerId to customer_id, totalspend to total_spend
3. save that in a new df

# COMMAND ----------

dffinal= df.withColumn("date",current_timestamp())\
    .withColumnRenamed("CustomerID","customer_id")\
        .withColumnRenamed("TotalSpend","total_spend")

# COMMAND ----------

dffinal.display()

# COMMAND ----------

Write:
1. file(csv,parquet, json, delta)
2. table 

# COMMAND ----------

# MAGIC %sql
# MAGIC create database test

# COMMAND ----------

# MAGIC %sql
# MAGIC use test

# COMMAND ----------

dffinal.write.saveAsTable("test.emp")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test.emp

# COMMAND ----------

# MAGIC %sql
# MAGIC select Gender,count(Gender) as count from emp group by Gender

# COMMAND ----------


