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
# MAGIC ## Exploring The Source Directory

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Auto Loader

# COMMAND ----------

##To work with Auto Loader, We use the readStream and writeStream methods from Spark structured streaming API. The format here is cloudFiles indicating that this is an Auto Loader stream. And in addition, we provide two options cloudFile.format where we precise that we are reading data files of type parquet. And the schemaLocation, a directory in which auto loader can store the information of the inferred schema. Then a load method where we provide the location of our data source files. 
# And we are chaining immediately the writeSteam to write the data into a target table, in our case, orders_updates. And of course, we provided the location for storing the checkpoint information, allowing auto loader to track the ingestion process.
# Before running the command, notice that we are using the same directory for storing both the schema and the checkpoints.
#once you run the below command it will be continuousely running and read whenever new data available in source. keep monitoring the dashboard charts
(spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", "dbfs:/mnt/demo/orders_checkpoint")
        .load(f"{dataset_bookstore}/orders-raw")
      .writeStream
        .option("checkpointLocation", "dbfs:/mnt/demo/orders_checkpoint")
        .table("orders_updates")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Once the data has been ingested to Delta Lake by auto loader, we can interact with it the same way we would with any table.
# MAGIC SELECT * FROM orders_updates

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM orders_updates

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Landing New Files

# COMMAND ----------

##run the below function twice to load two new files to the source and monitor the dashboard of the above cell
load_new_data()

# COMMAND ----------

#you would see new two files are appended to the source folder
files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC --both the new two files are procesed through auto loader
# MAGIC SELECT count(*) FROM orders_updates

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exploring Table History

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY orders_updates
# MAGIC --below output shows the streaming loads using the operation STREAMING UPDATE

# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/demo/orders_checkpoint")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Cleaning Up

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE orders_updates

# COMMAND ----------

dbutils.fs.rm("dbfs:/mnt/demo/orders_checkpoint", True)
