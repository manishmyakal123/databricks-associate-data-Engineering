# Databricks notebook source
# MAGIC %md
# MAGIC ## Transform Memberships Data
# MAGIC 1. Extract customer_id from the file path 
# MAGIC 1. Write transformed data to the Silver schema  

# COMMAND ----------

df_memberships = spark.table('gizmobox.bronze.py_memberships')
display(df_memberships)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Extract customer_id from the file path
# MAGIC > [Documentation for Regex Pattern](https://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html)

# COMMAND ----------

from pyspark.sql import functions as F
df_extracted_memberships = (
    df_memberships
        .select(
            F.regexp_extract("path", r".*/([0-9]+)\.png$", 1).alias("customer_id"),
            F.col("content").alias("membership_card")
        )
)
display(df_extracted_memberships)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Write transformed data to the Silver schema 

# COMMAND ----------

df_extracted_memberships.writeTo("gizmobox.silver.py_memberships").createOrReplace()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gizmobox.silver.py_memberships;