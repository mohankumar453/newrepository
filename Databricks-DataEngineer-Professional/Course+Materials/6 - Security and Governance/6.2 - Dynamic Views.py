# Databricks notebook source
# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC select 1

# COMMAND ----------

# MAGIC %sql 
# MAGIC --recall the columns of customers_silver table. Here email, first_name, last_name,street are PII data
# MAGIC DESCRIBE TABLE customers_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC --creating redacted view where PII data is being masked
# MAGIC CREATE OR REPLACE VIEW customers_vw AS
# MAGIC   SELECT
# MAGIC     customer_id,
# MAGIC     CASE 
# MAGIC       WHEN is_member('admin_demo') THEN email
# MAGIC       ELSE 'REDACTED'
# MAGIC     END AS email,
# MAGIC     gender,
# MAGIC     CASE 
# MAGIC       WHEN is_member('admin_demo') THEN first_name
# MAGIC       ELSE 'REDACTED'
# MAGIC     END AS first_name,
# MAGIC     CASE 
# MAGIC       WHEN is_member('admin_demo') THEN last_name
# MAGIC       ELSE 'REDACTED'
# MAGIC     END AS last_name,
# MAGIC     CASE 
# MAGIC       WHEN is_member('admin_demo') THEN street
# MAGIC       ELSE 'REDACTED'
# MAGIC     END AS street,
# MAGIC     city,
# MAGIC     country,
# MAGIC     row_time
# MAGIC   FROM customers_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC --I am not the member of admins_demo group. You can see those 4 fields are REDACTED
# MAGIC SELECT * FROM customers_vw

# COMMAND ----------

# MAGIC %sql
# MAGIC --after adding my account as a member of admins_demo group through settings --> Identity and access--> Groups --> create admins_demo group and add my account mohanalagesan12345@gmail.com to that group, I am able to see the PII data
# MAGIC SELECT * FROM customers_vw

# COMMAND ----------

# MAGIC %sql
# MAGIC --applying the redaced logic at row level i.e., row level security. So, member of admins_demo would be able to see records where country = "France" AND row_time > "2022-01-01"
# MAGIC --customers_fr_vw is created on top of customers_vw
# MAGIC CREATE OR REPLACE VIEW customers_fr_vw AS
# MAGIC SELECT * FROM customers_vw
# MAGIC WHERE 
# MAGIC   CASE 
# MAGIC     WHEN is_member('admin_demo') THEN TRUE
# MAGIC     ELSE country = "France" AND row_time > "2022-01-01"
# MAGIC   END

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_fr_vw
# MAGIC -- I am able to see the records for all the countries as I am part of admins_demo group

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM customers_fr_vw
# MAGIC --after removing me from the admins_demo group, I am able to see the records from other countries and PII are redacted
