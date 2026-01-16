-- Databricks notebook source
CREATE OR REFRESH STREAMING TABLE bronze_orders
COMMENT 'Raw orders data ingested from the source system operational data'
TBLPROPERTIES ('quality' = 'bronze')
AS
SELECT *,
       _metadata.file_path As input_file_path,
       current_timestamp() AS ingestion_time
FROM cloud_files(
  '/Volumes/circuitbox/landing/operational_data/orders/',
  'json',
  map('cloudFiles.inferColumnTypes','true')
)


-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE silver_orders_clean
(
  CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_order_status EXPECT (order_status IN ('Pending', 'Shipping', 'Cancelled', 'Completed')),
  CONSTRAINT valid_payment_method EXPECT (payment_method IN ('Credit Card', 'PayPal', 'Bank Transfer'))
)
COMMENT 'Cleaned orders data'
TBLPROPERTIES ('quality' = 'silver')
AS
SELECT customer_id,
       order_id,
       order_status,
       CAST(order_timestamp AS TIMESTAMP) AS order_timestamp,
       payment_method,
       items 
FROM STREAM(LIVE.bronze_orders)

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE silver_orders
COMMENT 'SILVER orders'
TBLPROPERTIES ('quality' = 'silver')
AS
SELECT customer_id,
       order_id,
       order_status,
       order_timestamp,
       payment_method,
       item.name AS item_name,
       item.item_id AS item_id,
       item.quantity AS item_quantity,
       item.price AS item_price,
       item.category AS item_category
FROM (
      SELECT customer_id,
       order_id,
       order_status,
       order_timestamp,
       payment_method,
       explode(items) AS item
      FROM STREAM(LIVE.silver_orders_clean)
)

