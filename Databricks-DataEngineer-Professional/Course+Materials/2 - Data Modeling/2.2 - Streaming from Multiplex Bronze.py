# Databricks notebook source
# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC describe bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC --cast key and value to string as they are in binary format
# MAGIC SELECT cast(key AS STRING), cast(value AS STRING)
# MAGIC FROM bronze
# MAGIC where topic = "orders"
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT v.*
# MAGIC FROM (
# MAGIC   SELECT from_json(cast(value AS STRING), "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>") v
# MAGIC   FROM bronze
# MAGIC   WHERE topic = "orders")

# COMMAND ----------

#create streaming view
(spark.readStream
      .table("bronze")
      .createOrReplaceTempView("bronze_tmp"))

# COMMAND ----------

# MAGIC %sql
# MAGIC --select from streaming view. This query will be running continously. Cancel the run since it is a streaming view
# MAGIC SELECT v.*
# MAGIC FROM (
# MAGIC   SELECT from_json(cast(value AS STRING), "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>") v
# MAGIC   FROM bronze_tmp
# MAGIC   WHERE topic = "orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC --create temporary view with orders topic data alone from streaming view bronze_tmp
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_silver_tmp AS
# MAGIC   SELECT v.*
# MAGIC   FROM (
# MAGIC     SELECT from_json(cast(value AS STRING), "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>") v
# MAGIC     FROM bronze_tmp
# MAGIC     WHERE topic = "orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC --cancel the run once it displays data as it is a streaming view
# MAGIC select * from orders_silver_tmp

# COMMAND ----------

# MAGIC %fs
# MAGIC ##getting issues when orders_silver was dropped and it requires checkpoint folder also to be dropped before recreating the table again
# MAGIC rm -r 'dbfs:/mnt/demo_pro/checkpoints/orders_silver'

# COMMAND ----------

#write only orders data to orders_silver table
query = (spark.table("orders_silver_tmp")
               .writeStream
               .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/orders_silver")
               .trigger(availableNow=True)
               .table("orders_silver"))

query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from orders_silver

# COMMAND ----------

#achieveing all the above using pyspark api
from pyspark.sql import functions as F

json_schema = "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"

query = (spark.readStream.table("bronze")
        .filter("topic = 'orders'")
        .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
        .select("v.*")
     .writeStream
        .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/orders_silver")
        .trigger(availableNow=True)
        .table("orders_silver"))

query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*)
# MAGIC FROM orders_silver

# COMMAND ----------


