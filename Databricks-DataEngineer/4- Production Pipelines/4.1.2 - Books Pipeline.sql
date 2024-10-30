-- Databricks notebook source
SET datasets.path=dbfs:/mnt/demo-datasets/bookstore;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## Bronze Layer Tables

-- COMMAND ----------

--creating streaming cdc table from cdc streaming data
CREATE OR REFRESH STREAMING LIVE TABLE books_bronze
COMMENT "The raw books data, ingested from CDC feed"
AS SELECT * FROM cloud_files("${datasets.path}/books-cdc", "json")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## Silver Layer Tables

-- COMMAND ----------

--APPLY CHANGES TO cannot create the table. Hence the table has to be created before running this command.
CREATE OR REFRESH STREAMING LIVE TABLE books_silver;

APPLY CHANGES INTO LIVE.books_silver
  FROM STREAM(LIVE.books_bronze)
  KEYS (book_id)
  APPLY AS DELETE WHEN row_status = "DELETE"
  SEQUENCE BY row_time
  COLUMNS * EXCEPT (row_status, row_time)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Gold Layer Tables

-- COMMAND ----------

--streaming tables or append only tables. Since books_silveris not a append only table i.e., cdc breaks append feature of the streaming table, this table further can not be refrenced by another streaming table. So, the author_counts_state table can not be created as streaming table.
CREATE LIVE TABLE author_counts_state
  COMMENT "Number of books per author"
AS SELECT author, count(*) as books_count, current_timestamp() updated_time
  FROM LIVE.books_silver
  GROUP BY author

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DLT Views

-- COMMAND ----------

--Like tables DLT views can be created using DLT pipeline and the data quality constraints can be mentioned for the views as well like we mentioned for tables in the earlier notebook
--Also orders_cleaned was created in 4.1 notebook. and it is referenced here. DLT pipeline can use multiple notebooks and based on the lineage of the tables, pipeline will be created
CREATE LIVE VIEW books_sales
  AS SELECT b.title, o.quantity
    FROM (
      SELECT *, explode(books) AS book 
      FROM LIVE.orders_cleaned) o
    INNER JOIN LIVE.books_silver b
    ON o.book.book_id = b.book_id;
