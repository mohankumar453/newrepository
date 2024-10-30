# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/orders.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

#check the count using read object and not readStream obhect
(spark.read
      .table("bronze")
      .filter("topic = 'orders'")
      .count()
)

# COMMAND ----------

#duplicates can be removed using dropDuplicates()
from pyspark.sql import functions as F

json_schema = "order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>"

batch_total = (spark.read
                      .table("bronze")
                      .filter("topic = 'orders'")
                      .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
                      .select("v.*")
                      .dropDuplicates(["order_id", "order_timestamp"])
                      .count()
                )

print(batch_total)

# COMMAND ----------

#dropDuplicates() can be used in structured streaming too. 
#Structured streaming can track state information for the unique keys in the data. This ensures that duplicate records do not exist within or between microbatches.However, over time, this state information will scale to represent all history. We can limit the amount of the state to be maintained by using Watermarking. Watermarking allows to only track state information for a window of time in which we expect records could be delayed. Here we define a watermark of 30 seconds.

#In this way, we are sure that there is no duplicate records exist in each new microbatches to be processed.However, when dealing with streaming duplication, there is another level of complexity compared to static data as each micro-batch is processed. We need also to ensure that records to be inserted are not already in the target table. We can achieve this using insert-only merge.
#https://www.databricks.com/blog/feature-deep-dive-watermarking-apache-spark-structured-streaming
#https://www.youtube.com/watch?v=xZHacxf5uZ8

deduped_df = (spark.readStream
                   .table("bronze")
                   .filter("topic = 'orders'")
                   .select(F.from_json(F.col("value").cast("string"), json_schema).alias("v"))
                   .select("v.*")
                   .withWatermark("order_timestamp", "30 seconds")
                   .dropDuplicates(["order_id", "order_timestamp"]))

# COMMAND ----------

#function to do merge which will avoid duplicates while loading
#this is for each microbatches
def upsert_data(microBatchDF, batch):
    microBatchDF.createOrReplaceTempView("orders_microbatch")
    
    sql_query = """
      MERGE INTO orders_silver a
      USING orders_microbatch b
      ON a.order_id=b.order_id AND a.order_timestamp=b.order_timestamp
      WHEN NOT MATCHED THEN INSERT *
    """
    
    microBatchDF.sparkSession.sql(sql_query)
    #microBatchDF._jdf.sparkSession().sql(sql_query)
    #spark.sql(sql_query) can be used --> However, in this particular case, the spark session cannot be accessed from within the microbatch process. Instead, we can access the local spark session from the microbatch data frame i.e, microBatchDF.sparkSession

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS orders_silver
# MAGIC (order_id STRING, order_timestamp Timestamp, customer_id STRING, quantity BIGINT, total BIGINT, books ARRAY<STRUCT<book_id STRING, quantity BIGINT, subtotal BIGINT>>)

# COMMAND ----------

#foreachBatch() used to pass the custom transformation
#Now, in order to code the upsert function in our stream, we need to use the foreachBatch method.This provides the option to execute custom data writing logic on each micro batch of a streaming data. In our case, this custom logic is the insert-only merge for deduplication.
#below code will apply the merge operation
#https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.streaming.DataStreamWriter.foreachBatch.html
query = (deduped_df.writeStream
                   .foreachBatch(upsert_data)
                   .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/orders_silver")
                   .trigger(availableNow=True)
                   .start())

query.awaitTermination()

# COMMAND ----------

#lets check the no. of rows loaded to the table after deduplication
#Indeed, the number of unique records match between our batch and streaming deduplication queries.
streaming_total = spark.read.table("orders_silver").count()

print(f"batch total: {batch_total}")
print(f"streaming total: {streaming_total}")

# COMMAND ----------


