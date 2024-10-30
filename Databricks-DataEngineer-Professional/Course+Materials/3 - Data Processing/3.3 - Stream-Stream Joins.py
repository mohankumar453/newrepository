# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/customers_orders.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

#here we are using "order_id", "customer_id" as a composite key for merging and in partitionBy()
#its again a scd1 type merge
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def batch_upsert(microBatchDF, batchId):
    window = Window.partitionBy("order_id", "customer_id").orderBy(F.col("_commit_timestamp").desc())
    
    (microBatchDF.filter(F.col("_change_type").isin(["insert", "update_postimage"]))
                 .withColumn("rank", F.rank().over(window))
                 .filter("rank = 1")
                 .drop("rank", "_change_type", "_commit_version")
                 .withColumnRenamed("_commit_timestamp", "processed_timestamp")
                 .createOrReplaceTempView("ranked_updates"))
    
    query = """
        MERGE INTO customers_orders c
        USING ranked_updates r
        ON c.order_id=r.order_id AND c.customer_id=r.customer_id
            WHEN MATCHED AND c.processed_timestamp < r.processed_timestamp
              THEN UPDATE SET *
            WHEN NOT MATCHED
              THEN INSERT *
    """
    
    microBatchDF.sparkSession.sql(query)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS customers_orders
# MAGIC (order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>, email STRING, first_name STRING, last_name STRING, gender STRING, street STRING, city STRING, country STRING, row_time TIMESTAMP, processed_timestamp TIMESTAMP)

# COMMAND ----------

#here we join orders_silver and customers_silver tables
#since customers_silver is a CDF enabled table, lets read this using starting version 2 from where it was CDF enabled
#When performing stream-stream join. Spark buffers past input as streaming state for both input streams, so that it can match every future input with past inputs and accordingly generate the joined results.
#And of course, similar to streaming duplication we saw before, we can limit the state using watermarks.
#after joining, we do merging
def porcess_customers_orders():
    orders_df = spark.readStream.table("orders_silver")
    
    cdf_customers_df = (spark.readStream
                             .option("readChangeData", True)
                             .option("startingVersion", 2)
                             .table("customers_silver")
                       )

    query = (orders_df
                .join(cdf_customers_df, ["customer_id"], "inner")
                .writeStream
                    .foreachBatch(batch_upsert)
                    .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/customers_orders")
                    .trigger(availableNow=True)
                    .start()
            )
    
    query.awaitTermination()
    
porcess_customers_orders()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_orders

# COMMAND ----------

#lets process another set of data
bookstore.load_new_data()
bookstore.process_bronze()
bookstore.porcess_orders_silver()
bookstore.porcess_customers_silver()

porcess_customers_orders()

# COMMAND ----------

# MAGIC %sql
# MAGIC --count is increased after the 2nd set of merge
# MAGIC SELECT count(*) FROM customers_orders

# COMMAND ----------


