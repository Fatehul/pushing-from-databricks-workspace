# Databricks notebook source
# MAGIC %md
# MAGIC ####mount adlsgen2 container using service principal for assignment01

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list('fatehulsecretscope')

# COMMAND ----------

# Generic function which will take care of Mounting and unmounting incase already mounted(production ready)
def storageAcnt_Mounting(storageaccount,container):
    client_id= dbutils.secrets.get(scope = 'fatehulsecretscope', key = 'assignment01-clientid')
    tenant_id= dbutils.secrets.get(scope = 'fatehulsecretscope', key = 'assignment01-tenantid')
    secret_value = dbutils.secrets.get(scope = 'fatehulsecretscope', key = 'assignment01-secretval')
# configurations
    configs = {"fs.azure.account.auth.type": "OAuth",
              "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
              "fs.azure.account.oauth2.client.id": client_id,
              "fs.azure.account.oauth2.client.secret": secret_value,
              "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
#mounting if no mount point exists
    alreadyMounted = False
    for x in dbutils.fs.mounts():
        if x.mountPoint == f'/mnt/{storageaccount}/{container}':
            alreadyMounted  = True
            break
        else:
            alreadyMounted = False
    print(alreadyMounted)
    if not alreadyMounted:
        dbutils.fs.mount(
            source = f"abfss://{container}@{storageaccount}.dfs.core.windows.net/",
            mount_point = f"/mnt/{storageaccount}/{container}",
            extra_configs = configs)
        alreadyMounted= True
        print("mounting done successfully")
    else:
        print("it is already mounted")
    display(dbutils.fs.mounts())


# COMMAND ----------

storageAcnt_Mounting('assignement01sa','fatehul-raw')

# COMMAND ----------

storageAcnt_Mounting('assignement01sa','fatehul-raw')

# COMMAND ----------

storageAcnt_Mounting('assignement01sa','fatehul-raw')

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType,FloatType

# COMMAND ----------

superstoreSchema = StructType(fields=[StructField("rowId", IntegerType(), False),
                                     StructField("orderId", StringType(), True),
                                     StructField("orderDate", StringType(), True),
                                     StructField("shipDate", StringType(), True),
                                     StructField("shipMode", StringType(), True),
                                     StructField("customerId", StringType(), True),
                                     StructField("customerName", StringType(), True),
                                     StructField("segment", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("city", StringType(), True),
                                     StructField("state", StringType(), True),
                                     StructField("postalCode", IntegerType(), True),
                                     StructField("region", StringType(), True),
                                     StructField("productId", StringType(), True),
                                     StructField("category", StringType(), True),
                                     StructField("subCategory", StringType(), True),
                                     StructField("productName", StringType(), True),
                                     StructField("sales", FloatType(), True),
                                     StructField("quantity", IntegerType(), True),
                                     StructField("discount", FloatType(), True),
                                     StructField("profit", FloatType(), True)
])

# COMMAND ----------

superstore_df = spark.read \
.option("header", True) \
.schema(superstoreSchema) \
.csv("/mnt/assignement01sa/fatehul-raw")

# COMMAND ----------

display(superstore_df)

# COMMAND ----------

val mountPoint = /mnt/assignement01sa/fatehul-raw

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists superstoredb 

# COMMAND ----------

superstore_df.write.mode("overwrite").format("delta").saveAsTable("superstoredb.superstoredeltatable")

# COMMAND ----------

# MAGIC %sql
# MAGIC use database superstoredb

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from superstoredb.superstoredeltatable

# COMMAND ----------

# MAGIC %sql
# MAGIC select segment, round(sum(sales)) total_sales
# MAGIC from superstoredb.superstoredeltatable
# MAGIC group by segment;

# COMMAND ----------


