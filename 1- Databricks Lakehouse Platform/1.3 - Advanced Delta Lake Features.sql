-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###time travel
-- MAGIC ###optmize
-- MAGIC ###vaccum commands

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Delta Time Travel

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

SELECT * 
FROM employees 

-- COMMAND ----------

SELECT * 
FROM employees VERSION AS OF 4
--Date before the update operation

-- COMMAND ----------

--another metho
SELECT * FROM employees@v4

-- COMMAND ----------

DELETE FROM employees

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

--rollback to pervios verstion
RESTORE TABLE employees TO VERSION AS OF 5

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## OPTIMIZE Command

-- COMMAND ----------

DESCRIBE DETAIL employees
--we have 5 small files
--optimise is used to combine all the small files into to single file

-- COMMAND ----------

OPTIMIZE employees
ZORDER BY id
--along with optimise we do zorder also based on id
--numFilesAdded: 1
--numFilesRemoved: 5

-- COMMAND ----------

DESCRIBE DETAIL employees
--now the no. of files after OPTIMIZE is just 1

-- COMMAND ----------

DESCRIBE HISTORY employees

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Except the latest file, all other files can be removed

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## VACUUM Command

-- COMMAND ----------

VACUUM employees

-- COMMAND ----------

-- MAGIC %md
-- MAGIC no files would be deleted by above command as the default retention period is 7 days. We need to mention the retention period

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

VACUUM employees RETAIN 0 HOURS

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false;

-- COMMAND ----------

VACUUM employees RETAIN 0 HOURS

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employees'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC not required files are successfully removed

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

SELECT * FROM employees@v1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Dropping Tables

-- COMMAND ----------

DROP TABLE employees

-- COMMAND ----------

SELECT * FROM employees

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employeesVAC'

-- COMMAND ----------

DESCRIBE HISTORY employees
