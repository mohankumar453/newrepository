# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/customers.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

#                 .filter(F.col("row_status").isin(["insert", "update"]))) --> only rows with inserts and update status are filterd out here as we are going to work on inserts and updates in this notebook. We will deal with deletes later
from pyspark.sql import functions as F

schema = "customer_id STRING, email STRING, first_name STRING, last_name STRING, gender STRING, street STRING, city STRING, country_code STRING, row_status STRING, row_time timestamp"

customers_df = (spark.table("bronze")
                 .filter("topic = 'customers'")
                 .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
                 .select("v.*")
                 .filter(F.col("row_status").isin(["insert", "update"])))

display(customers_df)
#you would see rows only with the row_status insert and update and there are rows with multiple update records. MERGE statement would fail when it encountrs multple updates for the same row. We can apply rank() to filter only the latest row based on the row_time

# COMMAND ----------

#
from pyspark.sql.window import Window

window = Window.partitionBy("customer_id").orderBy(F.col("row_time").desc())

ranked_df = (customers_df.withColumn("rank", F.rank().over(window))
                          .filter("rank == 1")
                          .drop("rank"))
display(ranked_df)

# COMMAND ----------

#below command will fail as the structured streaming does not support windows function. But this can be used over micro batch processing using custom function
ranked_df = (spark.readStream
                   .table("bronze")
                   .filter("topic = 'customers'")
                   .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
                   .select("v.*")
                   .filter(F.col("row_status").isin(["insert", "update"]))
                   .withColumn("rank", F.rank().over(window))
                   .filter("rank == 1")
                   .drop("rank")
             )

display(ranked_df)

# COMMAND ----------

#below logic seem to be scd1 logic. Hence target table should have unique customer_id after the inserts and updates
from pyspark.sql.window import Window

def batch_upsert(microBatchDF, batchId):
    window = Window.partitionBy("customer_id").orderBy(F.col("row_time").desc())
    
    (microBatchDF.filter(F.col("row_status").isin(["insert", "update"]))
                 .withColumn("rank", F.rank().over(window))
                 .filter("rank == 1")
                 .drop("rank")
                 .createOrReplaceTempView("ranked_updates"))
    
    query = """
        MERGE INTO customers_silver c
        USING ranked_updates r
        ON c.customer_id=r.customer_id
            WHEN MATCHED AND c.row_time < r.row_time
              THEN UPDATE SET *
            WHEN NOT MATCHED
              THEN INSERT *
    """
            # WHEN MATCHED AND c.row_time < r.row_time
            #   THEN UPDATE SET *
            # WHEN MATCHED AND r.row_status = "delete"  --> if we are interested to handle deletes we can add this
            #   THEN DELETE            
            # WHEN NOT MATCHED
            #   THEN INSERT 

    microBatchDF.sparkSession.sql(query)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS customers_silver
# MAGIC (customer_id STRING, email STRING, first_name STRING, last_name STRING, gender STRING, street STRING, city STRING, country STRING, row_time TIMESTAMP)

# COMMAND ----------

df_country_lookup = spark.read.json(f"{dataset_bookstore}/country_lookup")
display(df_country_lookup)

# COMMAND ----------

#we are joining with df_country_lookup to get the country name from df_country_lookup
query = (spark.readStream
                  .table("bronze")
                  .filter("topic = 'customers'")
                  .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
                  .select("v.*")
                  .join(F.broadcast(df_country_lookup), F.col("country_code") == F.col("code") , "inner")
               .writeStream
                  .foreachBatch(batch_upsert)
                  .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/customers_silver")
                  .trigger(availableNow=True)
                  .start()
          )

query.awaitTermination()

# COMMAND ----------

count = spark.table("customers_silver").count()
expected_count = spark.table("customers_silver").select("customer_id").distinct().count()
#customers_silver is supposed to have unique customer_id rows
#Notice here that we are using assert statement to verify if the table count meets our expected distinct count.
#Assertions are boolean expressions that check if a statement is true or false.
#They are used in unit tests to check if certain assumptions remain true while you are developing your code.
assert count == expected_count
print("Unit test passed.")
