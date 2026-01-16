# Databricks notebook source
# MAGIC %md
# MAGIC ## Transform Orders Data - String to JSON
# MAGIC 1. Pre-process the JSON String to fix the Data Quality Issues
# MAGIC 1. Transform JSON String to JSON Object
# MAGIC 1. Write transformed data to the silver schema

# COMMAND ----------

df_orders = spark.table('gizmobox.bronze.py_orders')
display(df_orders)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Pre-process the JSON String to fix the Data Quality Issues

# COMMAND ----------

from pyspark.sql import functions as F

df_fixed_orders = (
    df_orders.select (
        F.regexp_replace("value", '"order_date": (\\d{4}-\\d{2}-\\d{2})', '"order_date": "$1"').alias("fixed_value")
    )
)

display(df_fixed_orders)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Transform JSON String to JSON Object

# COMMAND ----------

df_with_schema = (
    df_fixed_orders.select (
        F.schema_of_json (F.col("fixed_value")).alias("schema")
    )
)
display (df_with_schema.limit(1))

# COMMAND ----------

orders_schema = '''STRUCT<customer_id: BIGINT, items: ARRAY<STRUCT<category: STRING, details: STRUCT<brand: STRING, color: STRING>, item_id: BIGINT, name: STRING, price: BIGINT, quantity: BIGINT>>, order_date: STRING, order_id: BIGINT, order_status: STRING, payment_method: STRING, total_amount: BIGINT, transaction_timestamp: STRING>'''

# COMMAND ----------

df_json_orders = (
    df_fixed_orders.select (
        F.from_json("fixed_value", orders_schema).alias("json_value")
    )
)
display(df_json_orders)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Write transformed data to the silver schema

# COMMAND ----------

df_json_orders.writeTo("gizmobox.silver.py_orders_json").createOrReplace()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gizmobox.silver.py_orders_json;