# Databricks notebook source
# MAGIC %md
# MAGIC ## Transform Addresses Data
# MAGIC 1. Create one record for each customer with 2 sets of address columns, 1 for shipping and 1 for billing address 
# MAGIC 1. Write transformed data to the Silver schema  

# COMMAND ----------

df_addresses = spark.table('gizmobox.bronze.py_addresses')
display(df_addresses)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Create one record for each customer with both addresses, one for each address_type

# COMMAND ----------

from pyspark.sql import functions as F
df_pivoted_addresses = (
    df_addresses
    .groupBy('customer_id')
    .pivot('address_type', ['billing', 'shipping'])
    .agg(
        F.max('address_line_1').alias('address_line_1'),
        F.max('city').alias('city'),
        F.max('state').alias('state'),
        F.max('postcode').alias('postcode')
    )
)
display(df_pivoted_addresses)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Write transformed data to the Silver schema 

# COMMAND ----------

df_pivoted_addresses.writeTo('gizmobox.silver.py_addresses').createOrReplace()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gizmobox.silver.py_addresses;