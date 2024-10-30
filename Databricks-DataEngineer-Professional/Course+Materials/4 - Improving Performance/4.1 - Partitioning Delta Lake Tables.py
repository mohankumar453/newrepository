# Databricks notebook source
# MAGIC %md
# MAGIC Partitioning is also useful when removing data older than a certain age from the table. For example, you can decide to delete the previous year's data. In this case, file deletion will be cleanly along partition boundaries.
# MAGIC
# MAGIC In the same way, data could be archived or backed up at partition boundaries to a cheaper storage tier. This drives a huge savings on a cloud storage.
# MAGIC But if you are using this table as a streaming source, remember that deleting data breaks the append-only requirement of streaming sources, which makes the table no more Streamable.
# MAGIC
# MAGIC To avoid this, you need to use the ignoreDeletes option when streaming from this table. This option enables the streaming processing from Delta tables with partition deletes. And of course, the deletion of files will not actually occur until you run VACUUM command on the table.

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC --get the location of the table first
# MAGIC DESCRIBE TABLE EXTENDED 
# MAGIC --dbfs:/user/hive/warehouse/bookstore_eng_pro.db/bronze --> this is the location

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/user/hive/warehouse/bookstore_eng_pro.db/bronze")
display(files)
#if you see in the output, under the name, you have the topic level partition folders along with delta_log folder

# COMMAND ----------

#below will give you the 2nd level partition folder details which is year_month
#As you can see, we have three partition directories that collectively comprise our bronze table. In addition to the Delta log directory. You can apply access control at directory level if necessary, for example, on the customers topic directory as it has Personally Identifiable Information or PII data.
#Let us take a look into this partition directory. Our second level partition was on the year_month column. As you can see, there are currently 19 directories present at this level. Remember at these partition boundaries that represent measures of time, you can easily delete or archive old data from the table.
files = dbutils.fs.ls("dbfs:/user/hive/warehouse/bookstore_eng_pro.db/bronze/topic=customers")
display(files)

# COMMAND ----------

#part files inside one partition    
files = dbutils.fs.ls("dbfs:/user/hive/warehouse/bookstore_eng_pro.db/bronze/topic=customers/year_month=2021-12/")
display(files)

# COMMAND ----------


