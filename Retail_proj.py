# Databricks notebook source
# DBTITLE 1,mount the storage
configs = {
    "fs.azure.account.key.<<storagename>>.blob.core.windows.net": "<<pass the storage key>>"
}
dbutils.fs.mount(
    source = "wasbs://<<containername>>@<<storagename>>.blob.core.windows.net/",
    mount_point = "/mnt/retail",
    extra_configs = configs
)

# COMMAND ----------

# DBTITLE 1,Read the bronze layer
df_transaction=spark.read.parquet('/mnt/retail/bronze/transaction')
df_product=spark.read.parquet('/mnt/retail/bronze/product')
df_store=spark.read.parquet('/mnt/retail/bronze/store')
df_customer=spark.read.parquet('/mnt/retail/bronze/customer/lipika27/Retail_multistore_project/refs/heads/main/')
display(df_product)



# COMMAND ----------

# DBTITLE 1,Silver dataset
from pyspark.sql.functions import col
df_transaction=df_transaction.select(
    col("transaction_id").cast("int"),
    col("customer_id").cast("int"),
    col("product_id").cast("int"),
    col("store_id").cast("int"),
    col("quantity").cast("int"),
    col("transaction_date").cast("date"),
)

df_product=df_product.select(
    col("product_id").cast("int"),
    col("product_name"),
    col("category"),
    col("price").cast("double"),
)

df_store=df_store.select(
    col("store_id").cast("int"),
    col("store_name"),
    col("location")
)
df_customer=df_customer.select("customer_id","first_name","last_name","email","city","registration_date"
).dropDuplicates(["customer_id"])


# COMMAND ----------

display(df_product)

# COMMAND ----------

# DBTITLE 1,Join all tables
df_silver = df_transaction \
    .join(df_customer, "customer_id") \
    .join(df_product, "product_id") \
    .join(df_store, "store_id",)\
    .withColumn("total_amount",col("quantity")*col("price"))
display(df_silver)


# COMMAND ----------

# DBTITLE 1,load data to silver
silver_path=("/mnt/retail/silver")
df_silver.write.mode("overwrite").format("delta").save(silver_path)


# COMMAND ----------

# DBTITLE 1,silver data set
spark.sql(f"""
  CREATE TABLE retail_silver_cleaned1
  USING DELTA
  LOCATION '/mnt/retail/silver';
""")

# COMMAND ----------

silver_df=spark.read.format('delta').load('/mnt/retail/silver/')
display(silver_df)

# COMMAND ----------

from pyspark.sql.functions import sum, countDistinct, avg

# Group and aggregate
df_gold = df_silver.groupBy(
    "transaction_date",
    "product_id",
    "product_name",
    "category",
    "store_id",
    "store_name",
    "location"
).agg(
    sum("quantity").alias("total_quantity_sold"),
    sum("total_amount").alias("total_sales_amount"),
    countDistinct("transaction_id").alias("number_of_transactions"),
    avg("total_amount").alias("avg_transaction_amount")
)

# Show result
display(df_gold)


# COMMAND ----------

# DBTITLE 1,upload in gold
gold_path=("/mnt/retail/gold")
df_gold.write.mode("overwrite").format("delta").save(gold_path)


# COMMAND ----------

spark.sql(f"""
  CREATE TABLE retail_gold_cleaned
  USING DELTA
  LOCATION '/mnt/retail/gold';
""")



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retail_gold_cleaned