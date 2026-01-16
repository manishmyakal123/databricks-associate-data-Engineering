# Databricks notebook source
# MAGIC %md
# MAGIC ## Stream Customers Data From Cloud Files to Delta Lake using Auto Loader
# MAGIC 1. Read files from cloud storage using Auto Loader
# MAGIC 1. Transform the dataframe to add the following columns
# MAGIC     -   file path: Cloud file path
# MAGIC     -   ingestion date: Current Timestamp
# MAGIC 1. Write the transformed data stream to Delta Lake Table

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Read files using Auto Loader

# COMMAND ----------

customers_df = (
                    spark.readStream
                         .format("cloudFiles")
                         .option("cloudFiles.format", "json")
                         .option("cloudFiles.schemaLocation", "/Volumes/gizmobox/landing/operational_data/customers_autoloader/_schema")
                         .option("cloudFiles.inferColumnTypes", "true")
                         .option("cloudFiles.schemaHints", "date_of_birth DATE, member_since DATE, created_timestamp TIMESTAMP")
                         .load("/Volumes/gizmobox/landing/operational_data/customers_autoloader/")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Transform the dataframe to add the following columns
# MAGIC - file path: Cloud file path
# MAGIC - ingestion date: Current Timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

customers_transformed_df = (
                                customers_df.withColumn("file_path", col("_metadata.file_path"))
                                            .withColumn("ingestion_date", current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Write the transformed data stream to Delta Table 

# COMMAND ----------

streaming_query = (
                    customers_transformed_df.writeStream
                        .format("delta")
                        .option("checkpointLocation", "/Volumes/gizmobox/landing/operational_data/customers_autoloader/_checkpoint_stream")
                        .toTable("gizmobox.bronze.customers_autoloader")
)

# COMMAND ----------

streaming_query.stop()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gizmobox.bronze.customers_autoloader;