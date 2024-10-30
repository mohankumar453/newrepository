-- Databricks notebook source
CREATE TABLE pii_test
(id INT, name STRING COMMENT "PII")
COMMENT "Contains PII"
TBLPROPERTIES ('contains_pii' = True)

-- COMMAND ----------

 DESCRIBE EXTENDED pii_test

-- COMMAND ----------

DESCRIBE DETAIL pii_test

-- COMMAND ----------

SHOW TBLPROPERTIES pii_test

-- COMMAND ----------

DESCRIBE HISTORY pii_test

-- COMMAND ----------

SHOW TABLES
