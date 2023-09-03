# Databricks notebook source


# COMMAND ----------

import datetime
users=[{"id":1, 
       "first_name":'Naval',
       "last_name":'Yemul',
        "mail":'navalyemul@mail.com',
        "is_customer":True,
        "amount_paid":1000.2,
        "customer_from":datetime.date(1991,5,3),
        "last_update_ts":datetime.date(2022,1,1)
       }, 
       {"id":2, 
       "first_name":'John',
       "last_name":'Players',
        "mail":'navalyemul@mail.com',
        "is_customer":True,
        "amount_paid":1000.2,
        "customer_from":datetime.date(1991,5,3),
        "last_update_ts":datetime.date(2022,1,1)
       },
       {"id":3, 
       "first_name":'Killer',
       "last_name":'Spykar',
        "mail":'navalyemul@mail.com',
        "is_customer":True,
        "amount_paid":1000.2,
        "customer_from":datetime.date(1991,5,3),
        "last_update_ts":datetime.date(2022,1,1)
       },
       {"id":4, 
       "first_name":'Levis',
       "last_name":'Jeans',
        "mail":'navalyemul@mail.com',
        "is_customer":True,
        "amount_paid":1000.2,
        "customer_from":datetime.date(1991,5,3),
        "last_update_ts":datetime.date(2022,1,1)
       },
       {"id":5, 
       "first_name":'Puma',
       "last_name":'Adidas',
        "mail":'navalyemul@mail.com',
        "is_customer":True,
        "amount_paid":1000.2,
        "customer_from":datetime.date(1991,5,3),
        "last_update_ts":datetime.date(2022,1,1)
       }
       
]

# COMMAND ----------

df=spark.createDataFrame(users)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation 

# COMMAND ----------

# MAGIC %md
# MAGIC #### DataFrame Function 
# MAGIC - .select()
# MAGIC - .alias()
# MAGIC - .withColumnRenamed()
# MAGIC - .toDF()
# MAGIC - .withColumn()
# MAGIC
# MAGIC #### Function 
# MAGIC - col
# MAGIC - concat
# MAGIC - lit
# MAGIC - explode

# COMMAND ----------

df1= df.select("id","first_name","last_name")

# COMMAND ----------

display(df1)

# COMMAND ----------

help(df.select)

# COMMAND ----------

df.select(col("id").alias("emp_id")).show()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.select("id",col("first_name"),df.last_name,df["mail"]).show()

# COMMAND ----------

help(concat)

# COMMAND ----------

df.select(concat("first_name",lit(" "),"last_name").alias("full_name")).show()

# COMMAND ----------

df.show()

# COMMAND ----------



# COMMAND ----------

df.select("*",col("id").alias("emp_id")).show()

# COMMAND ----------

df\
.withColumnRenamed("id","emp_id")\
.withColumnRenamed("first_name","fore_name")\
.withColumnRenamed("last_name","sur_name")\
.display()

# COMMAND ----------

help(df.withColumnRenamed)

# COMMAND ----------

df.columns

# COMMAND ----------

old_columns=['amount_paid',
'customer_from',
 'first_name',
 'id',
 'is_customer',
 'last_name',
 'last_update_ts',
 'mail'] ;\
 new_column=['amountpaid',
'customer_from',
 'fore_name',
 'emp_id',
 'is_customer',
 'sur_name',
 'last_update_ts',
 'e_mail']

# COMMAND ----------

 new_column=('amountpaid',
'customer_from',
 'fore_name',
 'emp_id',
 'is_customer',
 'sur_name',
 'last_update_ts',
 'e_mail')

# COMMAND ----------

new_column=(('amountpaid'),
('customer_from'),
 ('fore_name'),
 ('emp_id'),
 ('is_customer'),
 ('sur_name'),
 ('last_update_ts'),
 ('e_mail'))

# COMMAND ----------

new_column.dtypes

# COMMAND ----------

df.toDF(new_column).show()

# COMMAND ----------

help(df.toDF)

# COMMAND ----------

help(df.withColumn)

# COMMAND ----------

df.display()

# COMMAND ----------

df.withColumn("first_name",concat("first_name", lit(" "),"last_name")).show()

# COMMAND ----------

7

# COMMAND ----------


