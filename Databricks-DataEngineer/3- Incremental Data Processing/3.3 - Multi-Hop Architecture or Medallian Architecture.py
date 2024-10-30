# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exploring The Source dDirectory

# COMMAND ----------

#check files in source directory
files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Auto Loader

# COMMAND ----------

#Here, we are configuring a stream read on our Parquet source using Auto Loader with schema inference.And once configured, we immediately register a streaming temporary view "orders_raw_temp"to do data transformation in Spark SQL.
#Our stream has been created after running the command, but notice that it is not active yet until we do a display or write stream operation.
(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "dbfs:/mnt/demo/checkpoints/orders_raw")
    .load(f"{dataset_bookstore}/orders-raw")
    .createOrReplaceTempView("orders_raw_temp"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Enriching Raw Data

# COMMAND ----------

# MAGIC %sql
# MAGIC --orders_tmp will act as a streaming view here
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_tmp AS (
# MAGIC   SELECT *, current_timestamp() arrival_time, input_file_name() source_file
# MAGIC   FROM orders_raw_temp
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_tmp

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Bronze Table

# COMMAND ----------

#running incremental load to load orders_bronze
(spark.table("orders_tmp")
      .writeStream
      .format("delta")
      .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/orders_bronze")
      .outputMode("append")
      .table("orders_bronze"))

# COMMAND ----------

# MAGIC %sql
# MAGIC --3000 rows loaded as the source folder inital had 3 files each has 100 rows
# MAGIC SELECT count(*) FROM orders_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_bronze

# COMMAND ----------

#add new file using below function
load_new_data()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Creating Static Lookup Table

# COMMAND ----------

(spark.read
      .format("json")
      .load(f"{dataset_bookstore}/customers-json")
      .createOrReplaceTempView("customers_lookup"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_lookup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Silver Table

# COMMAND ----------

#create streaming temp view against bronze table
(spark.readStream
  .table("orders_bronze")
  .createOrReplaceTempView("orders_bronze_tmp"))

# COMMAND ----------

# MAGIC %sql
# MAGIC --doing some business transformations
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_enriched_tmp AS (
# MAGIC   SELECT order_id, quantity, o.customer_id, c.profile:first_name as f_name, c.profile:last_name as l_name,
# MAGIC          cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') AS timestamp) order_timestamp, books
# MAGIC   FROM orders_bronze_tmp o
# MAGIC   INNER JOIN customers_lookup c
# MAGIC   ON o.customer_id = c.customer_id
# MAGIC   WHERE quantity > 0)

# COMMAND ----------

#lets do stream write to enriched table
(spark.table("orders_enriched_tmp")
      .writeStream
      .format("delta")
      .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/orders_silver")
      .outputMode("append")
      .table("orders_silver"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM orders_silver

# COMMAND ----------

#trigger another new file and check the processing in silver layer
load_new_data()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Gold Table

# COMMAND ----------

(spark.readStream
  .table("orders_silver")
  .createOrReplaceTempView("orders_silver_tmp"))

# COMMAND ----------

# MAGIC %sql
# MAGIC --transformation on gold layer with some aggregations
# MAGIC CREATE OR REPLACE TEMP VIEW daily_customer_books_tmp AS (
# MAGIC   SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
# MAGIC   FROM orders_silver_tmp
# MAGIC   GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)
# MAGIC   )

# COMMAND ----------

#load the transformed data to gold table. Since we use availableNow=True, below will be processing all the data in micro batches and then stream will complete. In this way we can combine both streaming and batch workloads in the same pipelines
#And we are also using the "complete" output mode to rewrite the updated aggregation each time our logic runs. However, keep in mind that Structured Streaming assumes data is only being appended in the upstream tables. Once a table is updated or overwritten, it is no longer valid for streaming. So in our case here, we cannot read a stream from this gold table. Of course, to change this behavior, you can set options like ignoreChanges, but they have other limitations.
(spark.table("daily_customer_books_tmp")
      .writeStream
      .format("delta")
      .outputMode("complete")
      .option("checkpointLocation", "dbfs:/mnt/demo/checkpoints/daily_customer_books")
      .trigger(availableNow=True)
      .table("daily_customer_books"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM daily_customer_books

# COMMAND ----------

#add new file to load data to bronze, siver and gold layer. For gold layer, we need rerun spark.table.writeStream as it is designed to run in batch using trigger(availableNow=True)
load_new_data(all=True)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Stopping active streams

# COMMAND ----------

for s in spark.streams.active:
    print("Stopping stream: " + s.id)
    s.stop()
    s.awaitTermination()
