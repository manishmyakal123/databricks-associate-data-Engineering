-- Databricks notebook source
-- MAGIC %fs ls 'abfss://demo@deacourseextdlgizmobox.dfs.core.windows.net/'

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS dea_course_ext_dl_demo
    URL 'abfss://demo@deacourseextdlgizmobox.dfs.core.windows.net/'
    WITH (STORAGE CREDENTIAL dea_course_adls)
    COMMENT 'External Location for Demo Purposes'

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS demo
MANAGED LOCATION 'abfss://demo@deacourseextdlgizmobox.dfs.core.windows.net/'

-- COMMAND ----------

show catalogs

-- COMMAND ----------

USE CATALOG demo;

-- COMMAND ----------

SELECT current_catalog();

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Understanding Delta Lake Transaction Log
-- MAGIC Understand the cloud storage directory structure behind delta lake tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 0. Create a new schema under the demo catalog for this section of the course (delta_lake)

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS demo.delta_lake
    MANAGED LOCATION 'abfss://demo@deacourseextdlgizmobox.dfs.core.windows.net/delta_lake'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 1. Create a Delta Lake Table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS demo.delta_lake.companies
  (company_name STRING,
   founded_date DATE,
   country      STRING);

-- COMMAND ----------

DESC EXTENDED demo.delta_lake.companies;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 2. Insert some data

-- COMMAND ----------

INSERT INTO demo.delta_lake.companies
VALUES ("Apple", "1976-04-01", "USA");

-- COMMAND ----------

SELECT * FROM demo.delta_lake.companies;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 3. Insert some more data

-- COMMAND ----------

INSERT INTO demo.delta_lake.companies 
VALUES ("Microsoft", "1975-04-04", "USA"),
       ("Google", "1998-09-04", "USA"),
       ("Amazon", "1994-07-05", "USA");