-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Compaction using OPTIMIZE and Z-Ordering

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### OPTIMIZE Command
-- MAGIC
-- MAGIC > Merges multiple small files into fewer, larger files, thus improves the performance

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 1. Create table - demo.delta_lake.optimize_stock_prices

-- COMMAND ----------

DROP TABLE IF EXISTS demo.delta_lake.optimize_stock_prices;
CREATE TABLE IF NOT EXISTS demo.delta_lake.optimize_stock_prices
(
  stock_id STRING,
  price DOUBLE,
  trading_date DATE
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 2. Inserts some data in a few transactions

-- COMMAND ----------

INSERT INTO demo.delta_lake.optimize_stock_prices
VALUES ('AAPL', 227.65, "2025-02-10");

-- COMMAND ----------

INSERT INTO demo.delta_lake.optimize_stock_prices
VALUES ('GOOGL', 2775.0, "2025-02-10");

-- COMMAND ----------

INSERT INTO demo.delta_lake.optimize_stock_prices
VALUES ('MSFT', 325.0, "2025-02-10");

-- COMMAND ----------

INSERT INTO demo.delta_lake.optimize_stock_prices
VALUES ('AMZN', 3520.0, "2025-02-12");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 3. Check the table history

-- COMMAND ----------

DESC HISTORY demo.delta_lake.optimize_stock_prices;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 4. Check the number of files required to get the latest data

-- COMMAND ----------

DESC DETAIL demo.delta_lake.optimize_stock_prices;

-- COMMAND ----------

SELECT * FROM demo.delta_lake.optimize_stock_prices VERSION AS OF 3;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 5. Run OPTIMIZE

-- COMMAND ----------

OPTIMIZE demo.delta_lake.optimize_stock_prices;

-- COMMAND ----------

OPTIMIZE demo.delta_lake.optimize_stock_prices
ZORDER BY stock_id;