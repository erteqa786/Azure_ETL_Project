# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

# MAGIC %md
# MAGIC ### SILVER LAYER SCRIPT

# COMMAND ----------

# MAGIC %md
# MAGIC ## DATA LOADING
# MAGIC

# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type.awteststorageproject.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.awteststorageproject.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.awteststorageproject.dfs.core.windows.net", "app_id")
spark.conf.set("fs.azure.account.oauth2.client.secret.awteststorageproject.dfs.core.windows.net", "secret_value")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.awteststorageproject.dfs.core.windows.net", "https://login.microsoftonline.com/e7fefe29-b409-4c87-ba66-ce10f46f15d0/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading data

# COMMAND ----------

df_cal = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronze@awteststorageproject.dfs.core.windows.net/AdventureWorks_Calendar")
                                                                                          

# COMMAND ----------

df_cus = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronze@awteststorageproject.dfs.core.windows.net/AdventureWorks_Customers")

# COMMAND ----------

df_procat = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronze@awteststorageproject.dfs.core.windows.net/AdventureWorks_Product_Categories")

# COMMAND ----------

df_pro = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronze@awteststorageproject.dfs.core.windows.net/AdventureWorks_Products")

# COMMAND ----------

df_ret = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronze@awteststorageproject.dfs.core.windows.net/AdventureWorks_Returns")

# COMMAND ----------

df_sales = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronze@awteststorageproject.dfs.core.windows.net/AdventureWorks_Sales*")

# COMMAND ----------

df_ter = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronze@awteststorageproject.dfs.core.windows.net/AdventureWorks_Territories")

# COMMAND ----------

df_subcat = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronze@awteststorageproject.dfs.core.windows.net/Product_Subcategories")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tranformation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calender

# COMMAND ----------

df_cal.display()

# COMMAND ----------

df_cal = df_cal.withColumn("Month", month(col("Date")))\
             .withColumn("Year", year(col("Date")))
df_cal.display()                      


# COMMAND ----------

df_cal.write.format('parquet')\
             .mode('append')\
            .option("path","abfss://silver@awteststorageproject.dfs.core.windows.net/AdventureWorks_Calender")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customer
# MAGIC

# COMMAND ----------

df_cus.display()

# COMMAND ----------

df_cus.withColumn("fullName",concat(col('Prefix'),lit(' '),col('FirstName'),lit(' '),col('LastName'))).display()

# COMMAND ----------

df_cus=df_cus.withColumn("fullName", concat_ws(' ', col('Prefix'), col('FirstName'), col('LastName')))


# COMMAND ----------

df_cus.display()

# COMMAND ----------

df_cus.write.format('parquet')\
             .mode('append')\
            .option("path","abfss://silver@awteststorageproject.dfs.core.windows.net/AdventureWorks_Customers")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### sub categories
# MAGIC

# COMMAND ----------

df_subcat.display()

# COMMAND ----------

df_subcat.write.format('parquet')\
             .mode('append')\
            .option("path","abfss://silver@awteststorageproject.dfs.core.windows.net/AdventureWorks_Product_Categories")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Products

# COMMAND ----------

df_pro.display()

# COMMAND ----------

df_pro = df_pro.withColumn('ProductSKU',split(col('ProductSKU'),'-')[0])\
                .withColumn('ProductName',split(col('ProductName'),' ')[0])

# COMMAND ----------

df_pro.display()

# COMMAND ----------

df_pro.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@awteststorageproject.dfs.core.windows.net/AdventureWorks_Products")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Return

# COMMAND ----------

df_ret.display()

# COMMAND ----------

df_ret.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@awteststorageproject.dfs.core.windows.net/AdventureWorks_Returns")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Territories
# MAGIC

# COMMAND ----------

df_ter.display()

# COMMAND ----------

df_ter.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@awteststorageproject.dfs.core.windows.net/AdventureWorks_Territories")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### SALES

# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_sales = df_sales.withColumn('StockData',to_timestamp('StockDate'))

# COMMAND ----------

df_sales.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@awteststorageproject.dfs.core.windows.net/AdventureWorks_Sales")\
            .save()

# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_sales=df_sales.withColumn('OrderNumber',regexp_replace(col('OrderNumber'),'S','T'))

# COMMAND ----------

df_sales=df_sales.withColumn('multiply',col('OrderLineItem')*col('OrderQuantity'))
df_sales.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## SALES ANALYSIS

# COMMAND ----------

df_sales.groupBy('OrderDate').agg(count('OrderNumber').alias('TotalOrders')).display()


# COMMAND ----------

df_procat.display()

# COMMAND ----------

df_ter.display()