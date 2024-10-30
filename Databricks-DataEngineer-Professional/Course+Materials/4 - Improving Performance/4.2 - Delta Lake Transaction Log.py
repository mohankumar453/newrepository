# Databricks notebook source
# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

#list the files under the delta_log folder
files = dbutils.fs.ls("dbfs:/user/hive/warehouse/bookstore_eng_pro.db/bronze/_delta_log")
display(files)

# COMMAND ----------

display(spark.read.json("dbfs:/user/hive/warehouse/bookstore_eng_pro.db/bronze/_delta_log/00000000000000000001.json"))
#you can get the delta lake file statictics from the add column. It provides no. of records in the file and the min, max values, null count of the columns. min and max values for key and value columns are not shown. could be due to its binary format
#Delta Lake captures statistics in the transaction log for each added data file.
#Statistics will always be leveraged for file skipping. For ex. if we check for total record count of the table, instead of scanning the data, it will get the count from the transaction_log json files

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC SELECT *FROM bronze

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/user/hive/warehouse/bookstore_eng_pro.db/bronze/_delta_log")
display(files)

# COMMAND ----------

#during 10th commit, there will be a consoliadted checkpoint file created in native parquet format which collates the contents of all the files from 01.json to 10.json log files. This is to spead up the delta lake processing. 
#Databricks automatically creates Parquet checkpoint files every 10 commits to accelerate the resolution of the current table state.
display(spark.read.parquet("dbfs:/user/hive/warehouse/bookstore_eng_pro.db/bronze/_delta_log/00000000000000000010.checkpoint.parquet"))

# COMMAND ----------


