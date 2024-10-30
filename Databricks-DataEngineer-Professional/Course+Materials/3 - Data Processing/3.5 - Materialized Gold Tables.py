# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Professional/main/Includes/images/gold.png" width="60%">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC Lets create gold tables

# COMMAND ----------

# MAGIC %run ../Includes/Copy-Datasets

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE VIEW IF NOT EXISTS countries_stats_vw AS (
# MAGIC   SELECT country, date_trunc("DD", order_timestamp) order_date, count(order_id) orders_count, sum(quantity) books_count
# MAGIC   FROM customers_orders
# MAGIC   GROUP BY country, date_trunc("DD", order_timestamp)
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC --this is not a streaming view
# MAGIC --Remember a view is nothing but a SQL query against tables.So if you have a complex query with joins and subqueries, each time you query the view, Delta have to scan and join files from multiple tables, which would be really costly.
# MAGIC SELECT *
# MAGIC FROM countries_stats_vw
# MAGIC WHERE country = "France"
# MAGIC
# MAGIC -- Let us rerun this query and see how fast it is. Even if you have a view with complex logic, re-executing the view will be super fast on the currently active cluster. In fact, to save costs, Databricks uses a feature called Delta Caching. So subsequent execution of queries will use cached results. However, this result is not guaranteed to be persisted and is only cached for the currently active cluster. In a traditional databases, usually you can control cost associated with materializing results using materialized views.
# MAGIC
# MAGIC --In Databricks, the concept of a materialized view most closely maps to that of a gold table.Gold tables help to cut down the potential cost and latency associated with complex ad-hoc queries.
# MAGIC --1.78 seconds runtime --> 1st run
# MAGIC --1.23 seconds runtime --> 2nd run with reduced run time due to caching on cluster/delta caching
# MAGIC --0.96 seconds runtime

# COMMAND ----------

#Let us now create the gold table presented earlier in our architecture diagram. This table Stores Summary statistic of sales per author. It calculates the orders count and the average quantity per author and per order_timestamp for each non-overlapping five minutes interval. Furthermore, similar to streaming deduplication, we automatically handle late, out-of-order data, and limit the state using watermarks.
#Here we define a watermark of ten minutes during which incremental state information is maintained for late arriving data.
from pyspark.sql import functions as F

query = (spark.readStream
                 .table("books_sales")
                 .withWatermark("order_timestamp", "10 minutes")
                 .groupBy(
                     F.window("order_timestamp", "5 minutes").alias("time"),
                     "author")
                 .agg(
                     F.count("order_id").alias("orders_count"),
                     F.avg("quantity").alias ("avg_quantity"))
              .writeStream
                 .option("checkpointLocation", f"dbfs:/mnt/demo_pro/checkpoints/authors_stats")
                 .trigger(availableNow=True)
                 .table("authors_stats")
            )

query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM authors_stats
# MAGIC --Check the output. For example, for this autho 'Luciano Ramalho', We see his statistics for an interval of five minutes between 11:05 a.m. and 11:10 a.m..

# COMMAND ----------


