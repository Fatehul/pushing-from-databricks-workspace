# Databricks notebook source
data=[(1,'a',30),(2,'b',32),(3,'c',33)]

# COMMAND ----------

schema="id int, name string, age int"

# COMMAND ----------

df=spark.createDataFrame(data,schema)

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("naval.emp_demo")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from naval.emp_demo

# COMMAND ----------

data1=[(4,'d',30),(5,'e',32),(6,'f',33)]

# COMMAND ----------

df1=spark.createDataFrame(data1,schema)

# COMMAND ----------

df1.write.mode("append").saveAsTable("naval.emp_demo")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from naval.emp_demo

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history naval.emp_demo

# COMMAND ----------

data2=[(7,'d',30,'IT'),(8,'e',32,'Sales'),(8,'f',33,'Marketing')]

# COMMAND ----------

schema2="id int, name string, age int, dept string"

# COMMAND ----------

df2=spark.createDataFrame(data2, schema2)

# COMMAND ----------

df2.write.mode("append").saveAsTable("naval.emp_demo")

# COMMAND ----------

df2.write.mode("append").option("mergeSchema", "true").saveAsTable("naval.emp_demo")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from naval.emp_demo

# COMMAND ----------


