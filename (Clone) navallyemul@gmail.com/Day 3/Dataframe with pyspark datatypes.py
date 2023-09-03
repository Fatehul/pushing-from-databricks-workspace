# Databricks notebook source
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

schema= "id int,first_name string, last_name string, mail string, is_customer boolean ,amount_paid double, customer_from date,last_update_ts date "

# COMMAND ----------

df=spark.createDataFrame(users,schema)

# COMMAND ----------

import datetime
users2=[{"id":1, 
       "first_name":'Naval',
       "last_name":'Yemul',
        "mail":'navalyemul@mail.com',
        "phone_numbers": ["+1 234 567 789 ", "+91 123987645"],
        "is_customer":True,
        "amount_paid":1000.2,
        "customer_from":datetime.date(2002,12,31),
        "last_update_ts":datetime.date(2022,1,1)
       }, 
       {"id":2, 
       "first_name":'John',
       "last_name":'Players',
        "mail":'john@mail.com',
        "phone_numbers": ["+1 789 567 234 ", "+91 124137645"],
        "is_customer":True,
        "amount_paid":2000.4,
        "customer_from":datetime.date(2004,5,30),
        "last_update_ts":datetime.date(2022,11,29)
       },
       {"id":3, 
       "first_name":'Killer',
       "last_name":'Spykar',
        "mail":'killer@mail.com',
        "phone_numbers": [" ", "+91 124137645"],
        "is_customer":True,
        "amount_paid":3000.0,
        "customer_from":datetime.date(2016,3,18),
        "last_update_ts":datetime.date(2020,3,24)
       },
       {"id":4, 
       "first_name":'Levis',
       "last_name":'Jeans',
        "mail":'jeans@mail.com',
        "phone_numbers": ["+1 789 567 234 ", " "],
        "is_customer":True,
        "amount_paid":1500.5,
        "customer_from":datetime.date(1990,12,3),
        "last_update_ts":datetime.date(2022,3,2)
       },
       {"id":5, 
       "first_name":'Puma',
       "last_name":'Adidas',
        "mail":'puma@mail.com',
        "phone_numbers": [" ", " "],
        "is_customer":True,
        "amount_paid":500.5,
        "customer_from":datetime.date(1995,2,19),
        "last_update_ts":datetime.date(2022,1,21)
       }
       
]

# COMMAND ----------

schema2= "id int,first_name string, last_name string, mail string, phone_numbers array, is_customer boolean ,amount_paid double, customer_from date,last_update_ts date "

# COMMAND ----------

df2=spark.createDataFrame(users2,schema2)

# COMMAND ----------

display(df2)

# COMMAND ----------

-- struct
-- array
-- map 

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

users_schema1= StructType([StructField("id",IntegerType()),
                          StructField("first_name",StringType()),
                          StructField("last_name",StringType()),
                          StructField("mail",StringType()),
                          StructField("is_customer",BooleanType()),
                          StructField("amount_paid",DoubleType()),
                          StructField("customer_from",DateType()),
                          StructField("last_update_ts",DateType())
])

# COMMAND ----------

df=spark.createDataFrame(users,users_schema1)

# COMMAND ----------

display(df)

# COMMAND ----------

display(df2)

# COMMAND ----------

users_schema1= StructType([StructField("id",IntegerType()),
                          StructField("first_name",StringType()),
                          StructField("last_name",StringType()),
                          StructField("mail",StringType()),
                          StructField("phone_numbers",ArrayType(StringType())),
                          StructField("is_customer",BooleanType()),
                          StructField("amount_paid",DoubleType()),
                          StructField("customer_from",DateType()),
                          StructField("last_update_ts",DateType())
])

# COMMAND ----------

dfnew=spark.createDataFrame(users2,users_schema1)

# COMMAND ----------

display(dfnew)

# COMMAND ----------

dfnew.select("*", col("phone_numbers")[0]).display()

# COMMAND ----------

dfnew.withColumn("home_phone", col("phone_numbers")[0])\
    .withColumn("work_phone", col("phone_numbers")[1])\
    .drop("phone_numbers")\
.display()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

dfnew.withColumn("phone_numbers",explode("phone_numbers")).display()

# COMMAND ----------


