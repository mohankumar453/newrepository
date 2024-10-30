-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Querying JSON 

-- COMMAND ----------

-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/customers-json")
-- MAGIC display(files)

-- COMMAND ----------

-- to read json file
SELECT * FROM json.`${dataset.bookstore}/customers-json/export_001.json`

-- COMMAND ----------

--to read the all the files starting with export_*
SELECT * FROM json.`${dataset.bookstore}/customers-json/export_*.json`

-- COMMAND ----------

--to read all the files in a folder. Schema of all the files should be the same
SELECT * FROM json.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

SELECT count(*) FROM json.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

--input_file_name() function is used to extract the file name where the particular row exist
 SELECT *,
    input_file_name() AS source_file
  FROM json.`${dataset.bookstore}/customers-json`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Querying text Format

-- COMMAND ----------

--querying text editor files like json, csv, tsv,text with text.`filename`. It represents the row in single field which would be useful for debugging incase if the data in the file is corrupted
SELECT * FROM text.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Querying binaryFile Format

-- COMMAND ----------

--reading the data in binary format with some metadata details
SELECT * FROM binaryFile.`${dataset.bookstore}/customers-json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Querying CSV 

-- COMMAND ----------

--here the csv file is delimeted with ';'. Hence it is not read in a structured format. Additional options required for the same.
SELECT * FROM csv.`${dataset.bookstore}/books-csv`

-- COMMAND ----------

--below will create non delta table
CREATE TABLE books_csv
  (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  header = "true",
  delimiter = ";"
)
LOCATION "${dataset.bookstore}/books-csv"

-- COMMAND ----------

SELECT * FROM books_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Limitations of Non-Delta Tables

-- COMMAND ----------

--this is not a delta table as it is a external table. Hence you can not do time travel.
DESCRIBE EXTENDED books_csv

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ##lets try to add additional rows to the table books_csv
-- MAGIC (spark.read
-- MAGIC         .table("books_csv")
-- MAGIC       .write
-- MAGIC         .mode("append")
-- MAGIC         .format("csv")
-- MAGIC         .option('header', 'true')
-- MAGIC         .option('delimiter', ';')
-- MAGIC         .save(f"{dataset_bookstore}/books-csv"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #addtional files are created which start iwth part-000*
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/books-csv")
-- MAGIC display(files)

-- COMMAND ----------

--we're still unable to see this new data. And this is because Spark automatically cached the underlying data in local storage to ensure that on subsequent queries(in this case the original 4 files), Spark will provide the optimal performance by just querying this local cache. However, we can manually refresh the cache of our data by running the REFRESH TABLE command.
SELECT COUNT(*) FROM books_csv

-- COMMAND ----------

--But remember, refreshing a table will invalidate its cache, meaning that we will need to scan our original data source and pull all data back into memory. For a very large dataset, this may take a significant amount of time.
REFRESH TABLE books_csv

-- COMMAND ----------

--additional rows are showing up
SELECT COUNT(*) FROM books_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## CTAS Statements

-- COMMAND ----------

--So as you can see, non Delta tables have some limitations. To create Delta tables where we load data from external sources, we use
-- Create Table AS Select statements or CTAS statements. Below command will crate DELTA/MANAGED table
CREATE TABLE customers AS
SELECT * FROM json.`${dataset.bookstore}/customers-json`;

DESCRIBE EXTENDED customers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC From the table metadata, we can see that we are indeed creating a delta table, and it is also a managed table.
-- MAGIC In addition, we can see that schema has been inferred automatically from the query results. This is because CTAS statements automatically infer schema information from query results and do not support manual schema declaration. This means that CTAS statements are useful for external data ingestion from sources with well-defined schema such as parquet files and tables. In addition, CTAS statements do not support specifying additional file options which presents significant limitation when trying to ingest data from CSV files.

-- COMMAND ----------

--table is created below. but the data is not well parsed. We can add options using views
CREATE TABLE books_unparsed AS
SELECT * FROM csv.`${dataset.bookstore}/books-csv`;

SELECT * FROM books_unparsed;

-- COMMAND ----------

CREATE TEMP VIEW books_tmp_vw
   (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  path = "${dataset.bookstore}/books-csv/export_*.csv",
  header = "true",
  delimiter = ";"
);
--Below will create DELTA TABLE
CREATE TABLE books AS
  SELECT * FROM books_tmp_vw;
  
SELECT * FROM books

-- COMMAND ----------

DESCRIBE EXTENDED books
