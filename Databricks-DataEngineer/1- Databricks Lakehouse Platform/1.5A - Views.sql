-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Preparing Sample Data

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS smartphones
(id INT, name STRING, brand STRING, year INT);

INSERT INTO smartphones
VALUES (1, 'iPhone 14', 'Apple', 2022),
      (2, 'iPhone 13', 'Apple', 2021),
      (3, 'iPhone 6', 'Apple', 2014),
      (4, 'iPad Air', 'Apple', 2013),
      (5, 'Galaxy S22', 'Samsung', 2022),
      (6, 'Galaxy Z Fold', 'Samsung', 2022),
      (7, 'Galaxy S9', 'Samsung', 2016),
      (8, '12 Pro', 'Xiaomi', 2022),
      (9, 'Redmi 11T Pro', 'Xiaomi', 2022),
      (10, 'Redmi Note 11', 'Xiaomi', 2021)

-- COMMAND ----------

SHOW TABLES --shows objects from the default database

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating Stored Views

-- COMMAND ----------

CREATE VIEW view_apple_phones
AS  SELECT * 
    FROM smartphones 
    WHERE brand = 'Apple';

-- COMMAND ----------

SELECT * FROM view_apple_phones;

-- COMMAND ----------

SHOW TABLES; --view is persisted to the database and it is not a temporary object

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Creating Temporary Views

-- COMMAND ----------

CREATE TEMP VIEW temp_view_phones_brands
AS  SELECT DISTINCT brand
    FROM smartphones;



-- COMMAND ----------

SELECT * FROM temp_view_phones_brands;

-- COMMAND ----------

SHOW TABLES; --below output shows this view is temporary. Hence it is not persisted to ant DB

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Creating Global Temporary Views

-- COMMAND ----------

CREATE GLOBAL TEMP VIEW global_temp_view_latest_phones
AS SELECT * FROM smartphones
    WHERE year > 2020
    ORDER BY year DESC;

-- COMMAND ----------

SELECT * FROM global_temp.global_temp_view_latest_phones;

-- COMMAND ----------

SHOW TABLES; --this is not showing the global temp view. We need to run this command on global_temp schema

-- COMMAND ----------

SHOW TABLES IN global_temp; -- below output shows this is temporary object and temp_view_phones_brands is not tied to any database as it is not a global temp view or materialised(normal/stored views) view

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SELECT * FROM temp_view_phones_brands; --fails after cluster is restarted

-- COMMAND ----------

SHOW TABLES
