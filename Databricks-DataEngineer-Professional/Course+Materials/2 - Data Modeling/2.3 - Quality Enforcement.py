# Databricks notebook source
# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE orders_silver ADD CONSTRAINT timestamp_within_range CHECK (order_timestamp >= '2020-01-01');

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED orders_silver
# MAGIC --check the table properties from the below output. You would see the added constraint showing up

# COMMAND ----------

# MAGIC %sql
# MAGIC --Lets try to add one record that violates the constraint. Below will fail.
# MAGIC --As delta lake supports ACIC txns, txns are atomic. i.e., process will either succeeed or fail completely.
# MAGIC --that means although only one record violates the constraint, other records that are not violating the constraint also would not be loaded. This is called atomic.
# MAGIC INSERT INTO orders_silver
# MAGIC VALUES ('1', '2022-02-01 00:00:00.000', 'C00001', 0, 0, NULL),
# MAGIC        ('2', '2019-05-01 00:00:00.000', 'C00001', 0, 0, NULL),
# MAGIC        ('3', '2023-01-01 00:00:00.000', 'C00001', 0, 0, NULL)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Lets check if the rows are loaded or not
# MAGIC SELECT *
# MAGIC FROM orders_silver
# MAGIC WHERE order_id IN ('1', '2', '3')

# COMMAND ----------

# MAGIC %sql
# MAGIC --below query will fail. Because when the query try to add the constraint, it also chceks for existing rows if they are violiating the constraint or not. If violates, constraint will not be added to the table
# MAGIC --24 existing rows violates the contstraint
# MAGIC --option to make it succeed is delete the rows before adding constraint. or add the constraing before processing/loading orders_silver table
# MAGIC ALTER TABLE orders_silver ADD CONSTRAINT valid_quantity CHECK (quantity > 0);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED orders_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*)
# MAGIC FROM orders_silver
# MAGIC where quantity <= 0

# COMMAND ----------

from pyspark.sql import functions as F
#here we are filtering "quantity > 0" before writing to orders_silver
json_schema = "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"

query = (spark.readStream.table("bronze")
        .filter("topic = 'orders'")
        .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
        .select("v.*")
        .filter("quantity > 0")
     .writeStream
        .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/orders_silver")
        .trigger(availableNow=True)
        .table("orders_silver"))

query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*)
# MAGIC FROM orders_silver
# MAGIC where quantity <= 0

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop the constraint
# MAGIC ALTER TABLE orders_silver DROP CONSTRAINT timestamp_within_range;

# COMMAND ----------

# MAGIC %sql
# MAGIC --now constraint would not be showing in tbl properties as it is dropped
# MAGIC DESCRIBE EXTENDED orders_silver

# COMMAND ----------

# MAGIC %md
# MAGIC Lets drop the table and checkpoints as we will apply further transformations later

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE orders_silver

# COMMAND ----------

dbutils.fs.rm("dbfs:/mnt/demo_pro/checkpoints/orders_silver", True)

# COMMAND ----------


