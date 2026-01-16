-- Databricks notebook source
DESCRIBE EXTENDED demo.delta_lake.companies;

-- COMMAND ----------

-- MAGIC %fs ls 'abfss://demo@deacourseextdl.dfs.core.windows.net/delta_lake/__unitystorage/schemas/3a4b6eb6-1de9-4154-9932-f9ba58518ec2/tables/6997bc7d-685c-4b3c-a74d-4aa61f13f807'

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS demo.delta_lake.companies
  (company_name STRING,
   founded_date DATE,
   country      STRING);