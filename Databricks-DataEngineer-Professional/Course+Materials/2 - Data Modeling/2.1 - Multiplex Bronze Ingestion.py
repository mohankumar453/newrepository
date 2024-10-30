# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/bronze.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

#assume that the files comes from kafka streaming
files = dbutils.fs.ls(f"{dataset_bookstore}/kafka-raw")
display(files)

# COMMAND ----------

df_raw = spark.read.json(f"{dataset_bookstore}/kafka-raw")
display(df_raw)
#partition is the partition for the topic, offset is asigned for each record. 
#key and value are i binary format. It can be casted to string. timestamp is when the row is created on the kafka
#by default all the columns in json considered as string

# COMMAND ----------

#function to create bronse table
#availableNow=True --> this process will run in batch mode.
#Auto Loader provides a Structured Streaming source called cloudFiles. Given an input directory path on the cloud file storage, the cloudFiles source automatically processes new files as they arrive, with the option of also processing existing files in that directory.
from pyspark.sql import functions as F

def process_bronze():
  
    schema = "key BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp LONG"

    query = (spark.readStream
                        .format("cloudFiles")
                        .option("cloudFiles.format", "json")
                        .schema(schema)
                        .load(f"{dataset_bookstore}/kafka-raw")
                        .withColumn("timestamp", (F.col("timestamp")/1000).cast("timestamp"))  
                        .withColumn("year_month", F.date_format("timestamp", "yyyy-MM"))
                  .writeStream
                      .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/bronze")
                      .option("mergeSchema", True)
                      .partitionBy("topic", "year_month")
                      .trigger(availableNow=True)
                      .table("bronze"))
    
    query.awaitTermination()

# COMMAND ----------

#process the kafka streaming. This process will not be continuously running as this is a batch process
process_bronze()

# COMMAND ----------

batch_df = spark.table("bronze")
display(batch_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC --if you see the output, data is fed from multiple topics/feeds to a single feed. This is colled multiplex table.
# MAGIC SELECT DISTINCT(topic)
# MAGIC FROM bronze

# COMMAND ----------

#lets add new files to the spurce
bookstore.load_new_data()

# COMMAND ----------

#process the additional files
process_bronze()

# COMMAND ----------

# MAGIC %sql
# MAGIC --check the revised count now
# MAGIC SELECT COUNT(*) FROM bronze

# COMMAND ----------


