# Databricks notebook source
files = dbutils.fs.ls("dbfs:/mnt/demo/dlt/demo_bookstore")
display(files)
#dbfs:/mnt/demo/dlt/demo_bookstore/system/ - The system directory captures all the events associated with the pipeline.

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/mnt/demo/dlt/demo_bookstore/system/events")
display(files)
#events logs are stored as delta tables

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`dbfs:/mnt/demo/dlt/demo_bookstore/system/events`

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/mnt/demo/dlt/demo_bookstore/tables")
display(files)
#this will have all the DLT tables created

# COMMAND ----------

# MAGIC %sql
# MAGIC --demo_bookstore_dlt_db -- this is the db we defined during pipeline creation
# MAGIC SELECT * FROM demo_bookstore_dlt_db.cn_daily_customer_books

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demo_bookstore_dlt_db.fr_daily_customer_books

# COMMAND ----------


