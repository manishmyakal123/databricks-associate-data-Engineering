-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## User Defined Functions (UDF)
-- MAGIC
-- MAGIC   > - User Defined Functions (UDF) in Spark are custom functions created by users to extend the capabilities of Spark SQL and PySpark. 
-- MAGIC   > - UDFs allow us to perform calculations or transformations to apply business logic that are not possible with built-in functions
-- MAGIC   > - You define the function once and use it accross multiple queries.
-- MAGIC   > - SQL UDFs are recommended over Python UDFs due to better optimization
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Syntax
-- MAGIC -----------------------------------------------------------------------------
-- MAGIC **CREATE OR REPLACE FUNCTION** catalog_name.schema_name.udf_name(param_name data_type)   
-- MAGIC **RETURNS** return_type   
-- MAGIC **RETURN** expression;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### Write a function to concatenate firstname and surname to and output the fullname

-- COMMAND ----------

CREATE OR REPLACE FUNCTION gizmobox.default.get_fullname(firstname STRING, surname STRING)
RETURNS STRING
RETURN CONCAT(initcap(firstname), ' ', initcap(surname));

-- COMMAND ----------

SELECT gizmobox.default.get_fullname('john', 'smith');

-- COMMAND ----------

DESC FUNCTION EXTENDED gizmobox.default.get_fullname;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### Write a function to get the description of the payment status

-- COMMAND ----------

CREATE OR REPLACE FUNCTION gizmobox.default.get_payment_status(payment_status INT)
RETURNS STRING
RETURN CASE payment_status
         WHEN 1 THEN 'Success'
         WHEN 2 THEN 'Pending'
         WHEN 3 THEN 'Cancelled'
         WHEN 4 THEN 'Failed'
       END;

-- COMMAND ----------

SELECT payment_id,
       order_id,
       CAST(date_format(payment_timestamp,'yyyy-MM-dd') AS DATE) AS payment_date,
       date_format(payment_timestamp,'HH:mm:ss') AS payment_time,
       gizmobox.default.get_payment_status(payment_status) AS payment_status,  
       payment_method
  FROM gizmobox.bronze.payments;