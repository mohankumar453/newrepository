-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Parsing JSON Data

-- COMMAND ----------

SELECT * FROM customers
--Here column profile has nested json content(json within json)

-- COMMAND ----------

DESCRIBE customers
--profile column is of type string i.e., json string

-- COMMAND ----------

--use ':' to access the columns within json object
SELECT customer_id, profile:first_name, profile:address:country 
FROM customers

-- COMMAND ----------

--from_json is used to build struct type object from json. Below will fail as from_json needs schema to be defined
SELECT from_json(profile) AS profile_struct
  FROM customers;

-- COMMAND ----------

--select and copy the sample json row which does not have any null values for any columns and use this to define the schema
SELECT profile 
FROM customers 
LIMIT 1

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW parsed_customers AS
  SELECT customer_id, from_json(profile, schema_of_json('{"first_name":"Thomas","last_name":"Lane","gender":"Male","address":{"street":"06 Boulevard Victor Hugo","city":"Paris","country":"France"}}')) AS profile_struct
  FROM customers;
  
SELECT * FROM parsed_customers
--struct object is created as shown below

-- COMMAND ----------

DESCRIBE parsed_customers
--see the column profile_struct is mentioned as struct type

-- COMMAND ----------

SELECT customer_id, profile_struct.first_name, profile_struct.address.country
FROM parsed_customers
--ise dots'.' to acess the columns from struct object

-- COMMAND ----------

-- profile_struct.* will pull all the columns from struct object. Since addres is a struct within struct, it gets dispalyed as struct in the output
CREATE OR REPLACE TEMP VIEW customers_final AS
  SELECT customer_id, profile_struct.*
  FROM parsed_customers;
  
SELECT * FROM customers_final

-- COMMAND ----------

SELECT order_id, customer_id, books
FROM orders
--it has array column books which inturn contains json content for each array element

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Explode Function

-- COMMAND ----------

SELECT order_id, customer_id, explode(books) AS book 
FROM orders
--use explode to flatten the array. check the order_id 000000000004243 that has two rows now as it had two values in the array

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Collecting Rows

-- COMMAND ----------

--collect_set() is an aggregation function that allows to collect unique values for fields , including fields with in array. if there are multiple rows, then the columns are collected in array and dups are removed in the array. check the output of C0004 customer_id or the column orders_set to get to know the dup element removal in array
--books.book_id is collecting book_id along from json string. collect_set changes this to array.
SELECT customer_id,
  collect_set(order_id) AS orders_set,
  collect_set(books.book_id) AS books_set
FROM orders
GROUP BY customer_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ##Flatten Arrays

-- COMMAND ----------

SELECT customer_id,
  collect_set(books.book_id) As before_flatten,
  array_distinct(flatten(collect_set(books.book_id))) AS after_flatten
FROM orders
GROUP BY customer_id
--array_distinct to remove dups in array
--flatten to flatten the array of array i.e, combines array values and resuls in array again

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ##Join Operations

-- COMMAND ----------

CREATE OR REPLACE VIEW orders_enriched AS
SELECT *
FROM (
  SELECT *, explode(books) AS book 
  FROM orders) o
INNER JOIN books b
ON o.book.book_id = b.book_id;

SELECT * FROM orders_enriched

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Set Operations

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW orders_updates
AS SELECT * FROM parquet.`${dataset.bookstore}/orders-new`;

SELECT * FROM orders 
UNION 
SELECT * FROM orders_updates 

-- COMMAND ----------

--rerturns all rows found in both relation
--INTERSECT [ALL | DISTINCT]
SELECT * FROM orders 
INTERSECT 
SELECT * FROM orders_updates 

-- COMMAND ----------

--uncommon rows
--EXCEPT [ALL | DISTINCT ]
--EXCEPT and MINUS both are same
-- Returns the rows in subquery1 which are not in subquery2.
SELECT * FROM orders 
MINUS 
SELECT * FROM orders_updates 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Reshaping Data with Pivot

-- COMMAND ----------

--We can get the aggregated values based on a specific column values, which will be turned to multiple columns used in Select clause. The pivot table can be specified after the table name or subquery.
-- So here we have SELECT * FROM and we specify between two parenthesis the Select statement that will be the input for this table. In the pivot clause, the first argument is an aggregation function, and the column to be aggregated.
--Then we specify the pivot column in the FOR subclause. The IN operator contains the pivot columns values. So here we use the Pivot Command to create a new transactions table that flatten out the information contained in the orders table for each customer.Such a flatten data format can be useful for dashboarding, but also useful for applying machine learning algorithms for inference and predictions.
CREATE OR REPLACE TABLE transactions AS

SELECT * FROM (
  SELECT
    customer_id,
    book.book_id AS book_id,
    book.quantity AS quantity
  FROM orders_enriched
) PIVOT (
  sum(quantity) FOR book_id in (
    'B01', 'B02', 'B03', 'B04', 'B05', 'B06',
    'B07', 'B08', 'B09', 'B10', 'B11', 'B12'
  )
);

SELECT * FROM transactions
