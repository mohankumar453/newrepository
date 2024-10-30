-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Opening this notebook creates new spark session

-- COMMAND ----------

SHOW TABLES; --temporary views does not exit. so the temporary view created in another notebook is not accessible here

-- COMMAND ----------

SHOW TABLES IN global_temp; --global temp view created in another notebook still exist. This is because notebooks attached to same cluster can access the global temp view created using that cluster

-- COMMAND ----------

SELECT * FROM global_temp.global_temp_view_latest_phones; --if we restart the cluster, this command will fail. since the
--global temp view would be dropped if we detach/attache, restart the cluster

-- COMMAND ----------

SELECT * FROM global_temp.global_temp_view_latest_phones;  --this fails as the cluster was restarted before running this query

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Dropping Views

-- COMMAND ----------

DROP TABLE smartphones;

DROP VIEW view_apple_phones;
DROP VIEW global_temp.global_temp_view_latest_phones;

-- COMMAND ----------


