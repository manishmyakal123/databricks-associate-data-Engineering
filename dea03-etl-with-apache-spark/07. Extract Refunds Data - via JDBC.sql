-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Extract Data From the Returns SQL Table
-- MAGIC 1. Create Bronze Schema in Hive Metastore
-- MAGIC 1. Create External Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Create Bronze Schema in Hive Metastore

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS bronze;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Create External Table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS bronze.refunds
USING JDBC
OPTIONS (
  url 'jdbc:sqlserver://dea-gizmobox-sql-server.database.windows.net:1433;database=dea-gizmobox-db',
  dbtable 'refunds',
  user 'gizmoboxadm',
  password 'Manish@1234'
);  

-- COMMAND ----------

SELECT * FROM bronze.refunds;

-- COMMAND ----------

SELECT * FROM hive_metastore.bronze.refunds;

-- COMMAND ----------

DESC EXTENDED bronze.refunds

-- COMMAND ----------

DESC EXTENDED hive_metastore.bronze.refunds;