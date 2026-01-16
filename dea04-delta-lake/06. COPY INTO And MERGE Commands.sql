-- Databricks notebook source
-- MAGIC %md
-- MAGIC # COPY INTO and MERGE Commands

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### COPY INTO Command
-- MAGIC
-- MAGIC > - Incrementally loads data into Delta Lake tables from Cloud Storage  
-- MAGIC > - Supports schema evolution  
-- MAGIC > - Supports wide range of file formats (CSV, JSON, Parquet, Delta)  
-- MAGIC > - Alternative to Auto Loader for batch ingestion  
-- MAGIC
-- MAGIC Documentation - [COPY INTO](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/delta-copy-into)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create the table to copy the data into

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS demo.delta_lake.raw_stock_prices;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Incrementally load new files into the table

-- COMMAND ----------

DELETE FROM demo.delta_lake.raw_stock_prices;

COPY INTO demo.delta_lake.raw_stock_prices
FROM 'abfss://demo@deacourseextdl.dfs.core.windows.net/landing/stock_prices'
FILEFORMAT = JSON
FORMAT_OPTIONS ('inferSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- COMMAND ----------

SELECT * FROM demo.delta_lake.raw_stock_prices;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### MERGE Statement
-- MAGIC > - Used for upserts (Insert/ Update/ Delete operations in a single statement)
-- MAGIC > - Allows merging new data into a target table based on matching condition
-- MAGIC
-- MAGIC Documentation - [MERGE INTO](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/delta-merge-into)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create the table to merge the data into

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS demo.delta_lake.stock_prices
(
  stock_id STRING,
  price DOUBLE,
  trading_date DATE
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Merge the source data into target table
-- MAGIC 1. Insert new stocks received
-- MAGIC 2. Update price and trading_date if updates receieved
-- MAGIC 3. Delete stocks which are de-listed from the exchange (status = 'DELISTED')

-- COMMAND ----------

MERGE INTO demo.delta_lake.stock_prices AS target
USING demo.delta_lake.raw_stock_prices AS source
   ON target.stock_id = source.stock_id
WHEN MATCHED AND source.status = 'ACTIVE' THEN
    UPDATE SET target.price = source.price, target.trading_date = source.trading_date
WHEN MATCHED AND source.status = 'DELISTED' THEN    
    DELETE
WHEN NOT MATCHED AND source.status ='ACTIVE' THEN     
    INSERT (stock_id, price, trading_date) VALUES (source.stock_id, source.price, source.trading_date);

-- COMMAND ----------

SELECT * FROM demo.delta_lake.stock_prices;