# Databricks notebook source
# MAGIC %md
# MAGIC ## Extract Data From the Returns SQL Table
# MAGIC 1. Read Returns Data via JDBC
# MAGIC 2. Create Returns Table in Bronze Schema

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Read Returns Data via JDBC

# COMMAND ----------

df = (
  spark.read.format('jdbc')
       .option('url', 'jdbc:sqlserver://gizmobox-srv.database.windows.net:1433;database=gizmobox-db')
       .option('dbtable', 'refunds')
       .option('user', 'gizmoboxadm')
       .option('password', 'Gizmobox@Adm')
       .load()
)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Write the refunds data to the bronze schema

# COMMAND ----------

df.writeTo('gizmobox.bronze.py_refunds').createOrReplace()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.bronze.py_refunds;