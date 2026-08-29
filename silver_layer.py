# Databricks notebook source
# MAGIC %md
# MAGIC ### SILVER LAYER SCRIPT

# COMMAND ----------

# MAGIC %md
# MAGIC DATA ACCESS USING APP

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.adwrsdatalake.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.adwrsdatalake.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.adwrsdatalake.dfs.core.windows.net", "59afceb2-8a3b-4179-b176-f874d40212e1")
spark.conf.set("fs.azure.account.oauth2.client.secret.adwrsdatalake.dfs.core.windows.net", "xnu8Q~sCIiH5b6t2w34ihFEatrv51iuthGWAEckG")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.adwrsdatalake.dfs.core.windows.net", "https://login.microsoftonline.com/349e9d5d-7396-4844-ae69-28c55be1597e/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC DATA LOADING

# COMMAND ----------

# MAGIC %md
# MAGIC Reading the Data

# COMMAND ----------

df_cal = spark.read.format("csv")\
            .option("header",True)\
            .option("inferSchema",True)\
            .load("abfss://bronze@adwrsdatalake.dfs.core.windows.net/AdventureWorks_Calendar")

# COMMAND ----------

df_cus = spark.read.format("csv")\
            .option("header",True)\
            .option("inferSchema",True)\
            .load("abfss://bronze@adwrsdatalake.dfs.core.windows.net/AdventureWorks_Customers")

# COMMAND ----------

df_pro_cat = spark.read.format("csv")\
            .option("header",True)\
            .option("inferSchema",True)\
            .load("abfss://bronze@adwrsdatalake.dfs.core.windows.net/AdventureWorks_Products/AdventureWorks_Product_Categories.csv")

# COMMAND ----------

df_pro_subcat = spark.read.format("csv")\
            .option("header",True)\
            .option("inferSchema",True)\
            .load("abfss://bronze@adwrsdatalake.dfs.core.windows.net/AdventureWorks_Products/AdventureWorks_Product_Subcategories.csv")

# COMMAND ----------

df_pro = spark.read.format("csv")\
            .option("header",True)\
            .option("inferSchema",True)\
            .load("abfss://bronze@adwrsdatalake.dfs.core.windows.net/AdventureWorks_Products/AdventureWorks_Products.csv")

# COMMAND ----------

df_ret = spark.read.format("csv")\
            .option("header",True)\
            .option("inferSchema",True)\
            .load("abfss://bronze@adwrsdatalake.dfs.core.windows.net/AdventureWorks_Returns")

# COMMAND ----------

df_sal = spark.read.format("csv")\
            .option("header",True)\
            .option("inferSchema",True)\
            .load("abfss://bronze@adwrsdatalake.dfs.core.windows.net/AdventureWorks_Sales*")

# COMMAND ----------

df_ter = spark.read.format("csv")\
            .option("header",True)\
            .option("inferSchema",True)\
            .load("abfss://bronze@adwrsdatalake.dfs.core.windows.net/AdventureWorks_Territories")

# COMMAND ----------

# MAGIC %md
# MAGIC ### TRANSFORMATIONS

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------


#Transform caledar data

df_cal = df_cal.withColumn('month',month(col('Date')))
df_cal = df_cal.withColumn('year',year(col('Date')))
df_cal.write.format('parquet').mode('overwrite').option("path","abfss://silver@adwrsdatalake.dfs.core.windows.net/AdventureWorks_Calendar").save()

# COMMAND ----------


#Transform customer data

df_cus = df_cus.withColumn("FullName", concat_ws(" ", col("FirstName"), col("LastName")))
df_cus = df_cus.withColumn("Age", (datediff(current_date(), col("BirthDate")) / 365).cast('int'))
df_cus.write.format('parquet').mode('overwrite').option("path","abfss://silver@adwrsdatalake.dfs.core.windows.net/AdventureWorks_Customers").save()

# COMMAND ----------

#inject category data -- no need of transformation

df_pro_cat.write.format('parquet').mode('overwrite').option('path','abfss://silver@adwrsdatalake.dfs.core.windows.net/AdventureWorks_Product_Categories').save()

#inject sub category data -- no required of transformation

df_pro_subcat.write.format('parquet').mode('overwrite').option('path','abfss://silver@adwrsdatalake.dfs.core.windows.net/AdventureWorks_Product_Subcategories').save()


# COMMAND ----------

#Transform the prodcut data

df_pro = df_pro.withColumn('ProductSize',col('ProductSize').cast('int'))
df_pro = df_pro.withColumn('ProductSKU',split(col('ProductSKU'), "-")[0])
df_pro = df_pro.withColumn('ProductName', split(col('ProductName'), ",")[0])
df_pro.write.format('parquet').mode('overwrite').option('path','abfss://silver@adwrsdatalake.dfs.core.windows.net/AdventureWorks_Products').save()

# COMMAND ----------

#Transform returns data

df_ret.write.format('parquet').mode('overwrite').option('path','abfss://silver@adwrsdatalake.dfs.core.windows.net/AdventureWorks_Returns').save()

# COMMAND ----------

#Transform Terr data

df_ter = df_ter.withColumnRenamed('SalesTerritoryKey','TerritoryKey')
df_ter.write.format('parquet').mode('overwrite').option('path','abfss://silver@adwrsdatalake.dfs.core.windows.net/AdventureWorks_Territories').save()

# COMMAND ----------

#Transform sales data

df_sal = df_sal.withColumn('StockDate', to_timestamp(col('StockDate')))\
                .withColumn('mulkey', col('OrderLineItem')* col('OrderQuantity'))\
                .withColumn('OrderNumber', regexp_replace(col('OrderNumber'),'S','P'))
df_sal.write.format('parquet').mode('overwrite').option('path','abfss://silver@adwrsdatalake.dfs.core.windows.net/AdventureWorks_Sales').save()

# COMMAND ----------

# MAGIC %md
# MAGIC