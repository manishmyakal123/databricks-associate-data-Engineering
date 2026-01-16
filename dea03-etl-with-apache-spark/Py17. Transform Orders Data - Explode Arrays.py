# Databricks notebook source
# MAGIC %md
# MAGIC ## Transform Orders Data - Explode Arrays
# MAGIC 1. Access elements from the JSON object
# MAGIC 1. Deduplicate Array Elements
# MAGIC 1. Explode Arrays
# MAGIC 1. Write the Transformed Data to Silver Schema

# COMMAND ----------

df_orders = spark.table('gizmobox.silver.py_orders_json')
display(df_orders)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Access elements from the JSON object

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

df_orders_normalized = (
    df_orders
    .select (
        "json_value.order_id",
        "json_value.order_status",
        "json_value.payment_method",
        "json_value.total_amount",
        "json_value.transaction_timestamp",
        "json_value.customer_id",
        "json_value.items"
    )
)
display(df_orders_normalized)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Deduplicate Array Elements

# COMMAND ----------

df_orders_normalized = (
    df_orders
    .select (
        "json_value.order_id",
        "json_value.order_status",
        "json_value.payment_method",
        "json_value.total_amount",
        "json_value.transaction_timestamp",
        "json_value.customer_id",
        F.array_distinct("json_value.items").alias("items")
    )
)
display(df_orders_normalized)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 3. Explode Arrays

# COMMAND ----------

df_orders_exploded = (
    df_orders_normalized
    .select (
        "order_id",
        "order_status",
        "payment_method",
        "total_amount",
        "transaction_timestamp",
        "customer_id",
        F.explode("items").alias("item")
    )
)

display(df_orders_exploded)

# COMMAND ----------

df_order_items = (
    df_orders_exploded
    .select (
        "order_id",
        "order_status",
        "payment_method",
        "total_amount",
        "transaction_timestamp",
        "customer_id",
        "item.item_id",
        "item.name",
        "item.price",
        "item.quantity",
        "item.category",
        "item.details.brand",
        "item.details.color"
    )
)
display(df_order_items)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Write the Transformed Data to Silver Schema

# COMMAND ----------

df_order_items.writeTo("gizmobox.silver.py_orders").createOrReplace()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gizmobox.silver.py_orders;