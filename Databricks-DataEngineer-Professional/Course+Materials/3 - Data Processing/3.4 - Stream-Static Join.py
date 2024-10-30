# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/books_sales.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

#Our current_books table is no longer streamable. Refer 2.5 notebook. Remember this table is updated using batch overwrite logic so it breaks the requirement of an ever appending source for structured streaming.Fortunately, Delta Lake guarantees that the latest version of a static table is returned each time

#Remember when performing a stream-static join, the streaming portion of the join drives this join process.So only new data appearing on the streaming side of the query will trigger the processing. And we are guaranteed to get the latest version of the static table during each microbatch transaction.

#orders_silver(orders_df) is a streaming table and current_books(books_df) is a static table

#books_sales is written in append mode in a batch mode
from pyspark.sql import functions as F

def process_books_sales():
    
    orders_df = (spark.readStream.table("orders_silver")
                        .withColumn("book", F.explode("books"))
                )

    books_df = spark.read.table("current_books")

    query = (orders_df
                  .join(books_df, orders_df.book.book_id == books_df.book_id, "inner")
                  .writeStream
                     .outputMode("append")
                     .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/books_sales")
                     .trigger(availableNow=True)
                     .table("books_sales")
    )

    query.awaitTermination()
    
process_books_sales()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM books_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM books_sales

# COMMAND ----------

#lets add another set of data to current_books() that is static and not to orders_silver that is a streaming table
bookstore.load_new_data()
bookstore.process_bronze()
bookstore.porcess_books_silver()
bookstore.process_current_books()

process_books_sales()

# COMMAND ----------

# MAGIC %sql
# MAGIC --note that only changes on streaming table drive the join and not the changes on the static table. Hence there wont be any changes in the record count here
# MAGIC SELECT count(*) FROM books_sales

# COMMAND ----------

#lets add data to orders_silver streaming table
bookstore.porcess_orders_silver()

process_books_sales()

# COMMAND ----------

# MAGIC %sql
# MAGIC --Do note that the record count is increased as there were new records added to streaming table orders_silver and that had driven the stream-static join
# MAGIC SELECT count(*) FROM books_sales

# COMMAND ----------


