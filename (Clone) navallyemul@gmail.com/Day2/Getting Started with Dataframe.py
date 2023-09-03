# Databricks notebook source
# MAGIC %md
# MAGIC ##### Creating a DataFrame

# COMMAND ----------

## TWO ways

1. if you have a data 
df= spark.createDataFrame(data, schema)

2. if you have a file(csv, json, parquet, etc)
df=spark.read.csv("path")

# COMMAND ----------

users_data= [(1,'a',30),(2,'b',32)]

# COMMAND ----------

df=spark.createDataFrame(users_data)

# COMMAND ----------

df.show()

# COMMAND ----------

users_schema= "id int, name string, age int"

# COMMAND ----------

df1=spark.createDataFrame(users_data,users_schema)

# COMMAND ----------

df1.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### WHY SCHEMA?
# MAGIC 1. if there are no headers
# MAGIC 2. if there are no correct order of columns
# MAGIC 3. instead of inferschema, use your user defined schema. that will save lot of time 

# COMMAND ----------

# MAGIC %md
# MAGIC ### creating schema
# MAGIC 1. str
# MAGIC 2. list
# MAGIC 3. pyspark 

# COMMAND ----------

help(spark.createDataFrame)

# COMMAND ----------

from datetime import datetime, date
from pyspark.sql import Row

# COMMAND ----------

data=[
    Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
    Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
    Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
]

# COMMAND ----------

df = spark.createDataFrame(data)

# COMMAND ----------

df.show()

# COMMAND ----------

from datetime import datetime, date
import pandas as pd
from pyspark.sql import Row

df = spark.createDataFrame([
    Row(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
    Row(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0)),
    Row(a=4, b=5., c='string3', d=date(2000, 3, 1), e=datetime(2000, 1, 3, 12, 0))
])

# COMMAND ----------

df.show()

# COMMAND ----------

data3=[
    (1, 2., 'string1', date(2000, 1, 1), datetime(2000, 1, 1, 12, 0)),
    (2, 3., 'string2', date(2000, 2, 1), datetime(2000, 1, 2, 12, 0)),
    (3, 4., 'string3', date(2000, 3, 1), datetime(2000, 1, 3, 12, 0))
]

# COMMAND ----------

schema3='a long, b double, c string, d date, e timestamp'

# COMMAND ----------

df=spark.createDataFrame(data3,schema3)

# COMMAND ----------

df.show()

# COMMAND ----------

display(df)

# COMMAND ----------

df.display()

# COMMAND ----------

import datetime
users=[(1, 
       'Claire',
       'Gute',
        'clairegute@mail.com',
        True,
        1000.2,
        datetime.date(1991,5,3),
        datetime.date(2022,1,1)
       ), 
       (2, 
       'Brosina',
       'kodoli',
        'brosina@mail.com',
        False,
        2000.2,
        datetime.date(2000,4,3),
        datetime.date(2022,3,5)
       ),
       (3, 
       'Andrew',
       'Adam',
        'andrew@mail.com',
        True,
        1500.2,
        datetime.date(1995,10,21),
        datetime.date(2021,11,10)
       ),
       (4, 
       'John',
       'Allen',
        'john@mail.com',
        False,
        None,
        datetime.date(1997,6,30),
        datetime.date(2019,3,31)
       )
]

# COMMAND ----------

df3=spark.createDataFrame(users,users_schema)

# COMMAND ----------

users_schema='''
            id INT, 
            first_name String,
            last_name String,
            email string,
            is_customer Boolean, 
            amount_paid float,
            customer_from Date,
            last_update_ts date            
'''

# COMMAND ----------

df3.display()

# COMMAND ----------

df3.printSchema()

# COMMAND ----------

df3.dtypes

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/tables/hexaware/

# COMMAND ----------

df=spark.read.option("header",True).csv("dbfs:/FileStore/tables/hexaware/emp.csv")

# COMMAND ----------

df=spark.read.option("header",True).option("inferSchema",True).csv("dbfs:/FileStore/tables/hexaware/emp.csv")

# COMMAND ----------

df=spark.read.csv("dbfs:/FileStore/tables/hexaware/emp.csv",header=True,inferSchema=True)

# COMMAND ----------

df.display()
df.printSchema()

# COMMAND ----------

from pyspark.sql.functions  import *

# COMMAND ----------

display(df)

# COMMAND ----------

df_final=df.withColumn("ingestion",current_timestamp())\
.withColumnRenamed("CustomerID","cust_id")\
.withColumnRenamed("TotalSpend","total_spend")\
.dropDuplicates()\
.sort("cust_id")

# COMMAND ----------

df_final.display()

# COMMAND ----------

df_final.write.parquet("dbfs:/FileStore/tables/hexaware/output/empparquet")

# COMMAND ----------

df_final.write.csv("dbfs:/FileStore/tables/hexaware/output/empcsv")

# COMMAND ----------

pyspark
(Datalake: Azure(blob, ADLS), AWS(s3), DBFS, database, snowflake, lakehouse )
(format: csv,json,parquet,orc,avro,table,detlatable,excel,databasetable)
df=spark.read.csv("path")


T 


df.write.parquet("path")
(csv,json,parquet,orc,avro,table,detlatable,databasetable)
(ADLS, blob, s3, snowflake, synapse, lakehouse, dw, DBFS)

# COMMAND ----------

 ((dbfs)csv----df---transformation----parquet(dbfs))

# COMMAND ----------

pyspark
(Datalake: Azure(blob, ADLS), AWS(s3), DBFS, database, snowflake, lakehouse )
(format: csv,json,parquet,orc,avro,table,detlatable,excel,databasetable)
df=spark.read.csv("path")


T 


df.write.parquet("path")
(delta table)
(lakehouse)
