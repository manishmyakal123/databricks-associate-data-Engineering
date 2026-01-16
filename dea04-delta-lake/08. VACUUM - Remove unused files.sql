-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Remove unused files using VACUUM

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### VACUUM Command
-- MAGIC
-- MAGIC > Used to remove old, unused files from the Delta Lake to free up storage. Permanently deletes files that are no longer referenced in the transaction log and older than retention threshold (default 7 days)
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 1. Check the table history

-- COMMAND ----------

DESC HISTORY demo.delta_lake.optimize_stock_prices;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 2. Run VACUUM

-- COMMAND ----------

VACUUM demo.delta_lake.optimize_stock_prices;

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM demo.delta_lake.optimize_stock_prices RETAIN 0 HOURS;

-- COMMAND ----------

SELECT * FROM demo.delta_lake.optimize_stock_prices;

-- COMMAND ----------

SELECT * FROM demo.delta_lake.optimize_stock_prices VERSION AS OF 3;