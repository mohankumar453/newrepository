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
# MAGIC ## Reading Stream

# COMMAND ----------

#we use spark.readStream pyspark API to consider reading the delta table books as a streaming data and the data will be exposed using the streaming view books_streaming_tmp_vw.
(spark.readStream
      .table("books")
      .createOrReplaceTempView("books_streaming_tmp_vw")
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Displaying Streaming Data

# COMMAND ----------

# MAGIC %sql
# MAGIC --Below query will keep on running as it is a streaming view. It will display if any new data is loaded on the source
# MAGIC --so you need to manually cancel to stop the job
# MAGIC SELECT * FROM books_streaming_tmp_vw

# COMMAND ----------

# MAGIC %md
# MAGIC ## Applying Transformations

# COMMAND ----------

# MAGIC %sql
# MAGIC --applying aggregrations on the streaming data
# MAGIC --dashboards are used to monitor the data processing rate
# MAGIC SELECT author, count(book_id) AS total_books
# MAGIC FROM books_streaming_tmp_vw
# MAGIC GROUP BY author

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Unsupported Operations

# COMMAND ----------

# MAGIC %sql
# MAGIC --below query would fail a sorting(ORDER BY) and deduplication are not supported in streaming process. you need to use windowing and watermarking to achieve the same.
# MAGIC  SELECT * 
# MAGIC  FROM books_streaming_tmp_vw
# MAGIC  ORDER BY author

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Persisting Streaming Data

# COMMAND ----------

# MAGIC %sql
# MAGIC --author_counts_tmp_vw will again be a streaming view as it is created on top of another streaming view
# MAGIC --streaming view is converted to steaming temp view to use pyspark APIs down the line
# MAGIC CREATE OR REPLACE TEMP VIEW author_counts_tmp_vw AS (
# MAGIC   SELECT author, count(book_id) AS total_books
# MAGIC   FROM books_streaming_tmp_vw
# MAGIC   GROUP BY author
# MAGIC )

# COMMAND ----------

#writing stream data to table
(spark.table("author_counts_tmp_vw")                               
      .writeStream  
      .trigger(processingTime='4 seconds')
      .outputMode("complete")
      .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint")
      .table("author_counts")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Our data has been written to the target table, the author_counts table, and we can see that each author has currently only 1 book. And remember, what you see here is not a "streaming" query! simply because we are querying the table directly. I mean, not as a streaming source through a streaming DataFrame.
# MAGIC SELECT *
# MAGIC FROM author_counts

# COMMAND ----------

# MAGIC %md
# MAGIC ## Adding New Data

# COMMAND ----------

# MAGIC %sql
# MAGIC --add new data and verify the above output again. values for total_books for some of the rows would be changed
# MAGIC INSERT INTO books
# MAGIC values ("B19", "Introduction to Modeling and Simulation", "Mark W. Spong", "Computer Science", 25),
# MAGIC         ("B20", "Robot Modeling and Control", "Mark W. Spong", "Computer Science", 30),
# MAGIC         ("B21", "Turing's Vision: The Birth of Computer Science", "Chris Bernhardt", "Computer Science", 35)

# COMMAND ----------

# MAGIC %md
# MAGIC Always remember to cancel any active stream in your notebook, otherwise the stream will be always on and prevents the cluster from auto termination.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Streaming in Batch Mode 

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books
# MAGIC values ("B16", "Hands-On Deep Learning Algorithms with Python", "Sudharsan Ravichandiran", "Computer Science", 25),
# MAGIC         ("B17", "Neural Network Methods in Natural Language Processing", "Yoav Goldberg", "Computer Science", 30),
# MAGIC         ("B18", "Understanding digital signal processing", "Richard Lyons", "Computer Science", 35)

# COMMAND ----------

#In this scenario, we modify the trigger method to change our query from an always-on query triggered every 4 seconds to a triggered incremental batch. We do this using the availableNow trigger option.
#With this trigger option, the query will process all new available data and stop on its own after execution. In this case, we can use the awaitTermination method to block the execution of any cell in this notebook until the incremental batch's write has succeeded.
(spark.table("author_counts_tmp_vw")                               
      .writeStream           
      .trigger(availableNow=True)
      .outputMode("complete")
      .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint")
      .table("author_counts")
      .awaitTermination()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM author_counts

# COMMAND ----------

# MAGIC %md
# MAGIC ##Important
# MAGIC In Databricks Runtime 11.3 LTS and above, the Trigger.Once setting is deprecated. Databricks recommends you use Trigger.AvailableNow for all incremental batch processing workloads.

# COMMAND ----------


(spark.table("author_counts_tmp_vw")                               
      .writeStream           
      .trigger(once=True)
      .outputMode("complete")
      .option("checkpointLocation", "dbfs:/mnt/demo/author_counts_checkpoint")
      .table("author_counts")
      .awaitTermination()
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM author_counts
