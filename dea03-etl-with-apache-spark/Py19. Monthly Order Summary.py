# Databricks notebook source
# MAGIC %md
# MAGIC ## Monthly Order Summary
# MAGIC For each of the customer, produce the following summary per month
# MAGIC 1. total orders
# MAGIC 1. total items bought
# MAGIC 1. total amount spent

# COMMAND ----------

df_orders = spark.table('gizmobox.silver.py_orders')
display(df_orders)

# COMMAND ----------

from pyspark.sql import functions as F

df_order_summary = (
    df_orders
    .withColumn("order_month", F.date_format('transaction_timestamp', 'yyyy-MM'))
    .groupBy('order_month', 'customer_id')
    .agg(
        F.countDistinct('order_id').alias('total_orders'),
        F.sum('quantity').alias('total_items_bought'),
        F.sum(F.col('price') * F.col('quantity')).alias('total_amount')
    )
)

display(df_order_summary)

# COMMAND ----------

df_order_summary.writeTo('gizmobox.gold.py_order_summary_monthly').createOrReplace()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gizmobox.gold.py_order_summary_monthly;