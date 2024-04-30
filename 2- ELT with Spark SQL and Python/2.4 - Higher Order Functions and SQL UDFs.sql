-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

SELECT * FROM orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Filtering Arrays
-- MAGIC Higher order functions are used to deal with hierarchial data such as array, maps

-- COMMAND ----------

--you can use filter function here to filter the data from the array which is a struct type array
--extract rows where books.quantity >= 2

SELECT
  order_id,
  books,
  FILTER (books, i -> i.quantity >= 2) AS multiple_copies
FROM orders

-- COMMAND ----------

--since the above query results in emptry array for some rows, these empty array rows can be filtered using size()
SELECT order_id, multiple_copies
FROM (
  SELECT
    order_id,
    FILTER (books, i -> i.quantity >= 2) AS multiple_copies
  FROM orders)
WHERE size(multiple_copies) > 0;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Transforming Arrays

-- COMMAND ----------

--use TRANSFORM() to transform the elements in array.
--transformed column is of array type
SELECT
  order_id,
  books,
  TRANSFORM (
    books,
    b -> CAST(b.subtotal * 0.8 AS INT)
  ) AS subtotal_after_discount
FROM orders;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## User Defined Functions (UDF)

-- COMMAND ----------

--UDF allow you to register a custom combination of SQL logic as function in a database, making these methods reusable in any SQL query. In addition, UDF functions leverage spark SQL directly maintaining all the optimization of Spark when applying your custom logic to large datasets. At minimum, it requires a function name, optional parameters, the type to be returned, and some custom logic, of course. Our function here is named get_url, that accepts an email address as an argument and return a value of type string. 

--Note that user defined functions are permanent objects that are persisted to the database, so you can use them between different Spark sessions and notebooks. With Describe Function command, we can see where it was registered and basic information about expected inputs and the expected return type. As you can see, our function, it belongs to the default database and accepts the email address as a string input, and returns a string value. We can get even more information by running Describe Function Extended. For example, the Body field at the bottom shows the SQL logic used in the function itself.

CREATE OR REPLACE FUNCTION get_url(email STRING)
RETURNS STRING --UDF returns STRING type data

RETURN concat("https://www.", split(email, "@")[1])

-- COMMAND ----------

SELECT email, get_url(email) domain
FROM customers

-- COMMAND ----------

--this says it sa type function and to get more details try DESCRIBE FUNCTION EXTENDED
DESCRIBE FUNCTION get_url

-- COMMAND ----------

--below query shows the body of the function too.--> Body:          concat("https://www.", split(email, "@")[1])
DESCRIBE FUNCTION EXTENDED get_url

-- COMMAND ----------

CREATE FUNCTION site_type(email STRING)
RETURNS STRING
RETURN CASE 
          WHEN email like "%.com" THEN "Commercial business"
          WHEN email like "%.org" THEN "Non-profits organization"
          WHEN email like "%.edu" THEN "Educational institution"
          ELSE concat("Unknow extenstion for domain: ", split(email, "@")[1])
       END;

-- COMMAND ----------

SELECT email, site_type(email) as domain_category
FROM customers

-- COMMAND ----------

DROP FUNCTION get_url;
DROP FUNCTION site_type;
