# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/CDF.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC --enable delta table with CDF. This can be set at the time of creating the table or at the notebook/cluster level setting set spark.databricks.delta.properties.defaults.enableChangeDataFeed = true;--> this is SQL way
# MAGIC ALTER TABLE customers_silver 
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC --check the tble properties to verify if the table is CDF enabled. 
# MAGIC --make a note of the version of it which is 2
# MAGIC DESCRIBE TABLE EXTENDED customers_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC --When CDF enabled, table is updated to newer version. Check the operation column in the output
# MAGIC DESCRIBE HISTORY customers_silver

# COMMAND ----------

#add new feed to process bronze, silver tables
bookstore.load_new_data()
bookstore.process_bronze()
bookstore.porcess_orders_silver()
bookstore.porcess_customers_silver()

# COMMAND ----------

# MAGIC %sql
# MAGIC --we need to read the table from the version from where it is CDF enabled.
# MAGIC --SELECT * FROM table_changes('table_name’, start_version, [end_version])
# MAGIC SELECT * 
# MAGIC FROM table_changes("customers_silver", 2) --2 is a starting version
# MAGIC --check the columns _change_type, _commit_version, _commit_timestamp at the end which is popluted by the CDF process
# MAGIC --update_preimage before the update image

# COMMAND ----------

#Lets load another feed in the process
bookstore.load_new_data()
bookstore.process_bronze()
bookstore.porcess_orders_silver()
bookstore.porcess_customers_silver()

# COMMAND ----------

#in python, you can use options "readChangeData" and "startingVersion" to read the CDF changes
#since it is a readstream, it will be running continously. Hence cancel it
cdf_df = (spark.readStream
               .format("delta")
               .option("readChangeData", True)
               .option("startingVersion", 2)
               .table("customers_silver"))

display(cdf_df)
#check the commit version in the output which is changed from 3 to 4 as it encountered another set of updates.

# COMMAND ----------

#you will see additional folder _change_data/ along with _delta_log/
files = dbutils.fs.ls("dbfs:/user/hive/warehouse/bookstore_eng_pro.db/customers_silver")
display(files)

# COMMAND ----------

#_change_data folder has data pertains to changes happened for CDF
files = dbutils.fs.ls("dbfs:/user/hive/warehouse/bookstore_eng_pro.db/customers_silver/_change_data")
display(files)

# COMMAND ----------


