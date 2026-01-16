# Databricks notebook source
# MAGIC %md
# MAGIC ## Extract Data From the Payments Files
# MAGIC 1. Read Payments File
# MAGIC 1. Create Payments Table in Bronze Schema

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. List the files from payment folder

# COMMAND ----------

# MAGIC %fs ls 'abfss://gizmobox@deacourseextdlgizmobox.dfs.core.windows.net/landing/external_data/payments'

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Read the payments file

# COMMAND ----------

payments_schema = 'payment_id INTEGER, order_id INTEGER, payment_timestamp TIMESTAMP, payment_status INTEGER, payment_method STRING'

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

py_payments_schema = StructType([
    StructField("payment_id", IntegerType()),
    StructField("order_id", IntegerType()),
    StructField("payment_timestamp", TimestampType()),
    StructField("payment_status", IntegerType()),
    StructField("payment_method", StringType())
])

# COMMAND ----------

df = (
    spark.read.format('csv')
         .option('delimiter', ',')
         .schema(py_payments_schema)
         .load('abfss://gizmobox@deacourseextdlgizmobox.dfs.core.windows.net/landing/external_data/payments')
)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Create Payment Table in the bronze schema

# COMMAND ----------

df.writeTo('gizmobox.bronze.py_payments').createOrReplace()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gizmobox.bronze.py_payments;