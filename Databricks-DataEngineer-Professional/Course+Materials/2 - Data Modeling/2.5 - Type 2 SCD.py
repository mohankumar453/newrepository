# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/books.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC --sample merge logic to apply scd2 updates based on the changes in price
# MAGIC MERGE INTO books_silver
# MAGIC USING (
# MAGIC     SELECT updates.book_id as merge_key, updates.*
# MAGIC     FROM updates
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT NULL as merge_key, updates.*
# MAGIC     FROM updates
# MAGIC     JOIN books_silver ON updates.book_id = books_silver.book_id
# MAGIC     WHERE books_silver.current = true AND updates.price <> books_silver.price
# MAGIC   ) staged_updates
# MAGIC ON books_silver.book_id = merge_key 
# MAGIC WHEN MATCHED AND books_silver.current = true AND books_silver.price <> staged_updates.price THEN
# MAGIC   UPDATE SET current = false, end_date = staged_updates.updated
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (book_id, title, author, price, current, effective_date, end_date)
# MAGIC   VALUES (staged_updates.book_id, staged_updates.title, staged_updates.author, staged_updates.price, true, staged_updates.updated, NULL)

# COMMAND ----------

#applying merge in function
def type2_upsert(microBatchDF, batch):
    microBatchDF.createOrReplaceTempView("updates")
    
    sql_query = """
        MERGE INTO books_silver
        USING (
            SELECT updates.book_id as merge_key, updates.*
            FROM updates

            UNION ALL

            SELECT NULL as merge_key, updates.*
            FROM updates
            JOIN books_silver ON updates.book_id = books_silver.book_id
            WHERE books_silver.current = true AND updates.price <> books_silver.price
          ) staged_updates
        ON books_silver.book_id = merge_key 
        WHEN MATCHED AND books_silver.current = true AND books_silver.price <> staged_updates.price THEN
          UPDATE SET current = false, end_date = staged_updates.updated
        WHEN NOT MATCHED THEN
          INSERT (book_id, title, author, price, current, effective_date, end_date)
          VALUES (staged_updates.book_id, staged_updates.title, staged_updates.author, staged_updates.price, true, staged_updates.updated, NULL)
    """
    
    microBatchDF.sparkSession.sql(sql_query)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS books_silver
# MAGIC (book_id STRING, title STRING, author STRING, price DOUBLE, current BOOLEAN, effective_date TIMESTAMP, end_date TIMESTAMP)

# COMMAND ----------

#pass type2_upsert to foreachBatch to apply the merge on micro batches and call process_books()
def process_books():
    schema = "book_id STRING, title STRING, author STRING, price DOUBLE, updated TIMESTAMP"
 
    query = (spark.readStream
                    .table("bronze")
                    .filter("topic = 'books'")
                    .select(F.from_json(F.col("value").cast("string"), schema).alias("v"))
                    .select("v.*")
                 .writeStream
                    .foreachBatch(type2_upsert)
                    .option("checkpointLocation", "dbfs:/mnt/demo_pro/checkpoints/books_silver")
                    .trigger(availableNow=True)
                    .start()
            )
    
    query.awaitTermination()
    
process_books()

# COMMAND ----------

books_df = spark.read.table("books_silver").orderBy("book_id", "effective_date")
display(books_df)
#you will see only inserted rows as books_silver is loaded for the first time

# COMMAND ----------

#add the second set of files and process it in bronze table then apply scd2
bookstore.load_books_updates()
bookstore.process_bronze()
process_books()

# COMMAND ----------

books_df = spark.read.table("books_silver").orderBy("book_id", "effective_date")
display(books_df)

# COMMAND ----------

#add the third set of files and process it in bronze table then apply scd2
bookstore.load_books_updates()
bookstore.process_bronze()
process_books()

# COMMAND ----------

books_df = spark.read.table("books_silver").orderBy("book_id", "effective_date")
display(books_df)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/current_books.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC Lets create current_books table with only active records as per the architecture

# COMMAND ----------

# MAGIC %sql
# MAGIC --every time the table will be overwritten
# MAGIC CREATE OR REPLACE TABLE current_books
# MAGIC AS SELECT book_id, title, author, price
# MAGIC    FROM books_silver
# MAGIC    WHERE current IS TRUE

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM current_books
# MAGIC ORDER BY book_id
