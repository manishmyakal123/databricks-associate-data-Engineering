-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Configure Access to Azure SQL Database via Lakehouse Federation
-- MAGIC 1. Create Connection - asql_gizmobox_db_conn_sql
-- MAGIC 1. Create Foreign Catalog - asql_gizmobox_db_catalog_sql

-- COMMAND ----------

DROP CATALOG asql_gizmobox_db_catalog_sql;


-- COMMAND ----------

Drop connection if exists asql_gizmobox_db_conn_sql;
CREATE CONNECTION asql_gizmobox_db_conn_sql TYPE sqlserver
OPTIONS (
  host 'dea-gizmobox-sql-server.database.windows.net',
  port '1433',
  user 'gizmoboxadm',
  password 'Manish@1234'
);

-- COMMAND ----------

CREATE FOREIGN CATALOG IF NOT EXISTS asql_gizmobox_db_catalog_sql USING CONNECTION asql_gizmobox_db_conn_sql
OPTIONS (database 'dea-gizmobox-db');

-- COMMAND ----------

SHOW CATALOGS;

-- COMMAND ----------

USE CATALOG asql_gizmobox_db_catalog_sql;

-- COMMAND ----------

SHOW SCHEMAS;

-- COMMAND ----------

USE SCHEMA dbo;

-- COMMAND ----------

SHOW CATALOGS;
USE CATALOG asql_gizmobox_db_catalog_sql;
SHOW SCHEMAS;
USE SCHEMA dbo;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC %sh
-- MAGIC    nc -zv dea-gizmobox-sql-server.database.windows.net 1433

-- COMMAND ----------

SELECT p.*, r.*
  FROM asql_gizmobox_db_catalog_sql.dbo.refunds r 
  JOIN gizmobox.silver.payments p ON r.payment_id = p.payment_id;