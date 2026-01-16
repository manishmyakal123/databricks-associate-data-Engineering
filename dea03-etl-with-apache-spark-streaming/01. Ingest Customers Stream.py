# Databricks notebook source
# MAGIC %md
# MAGIC ## Stream Customers Data From Cloud Files to Delta Lake
# MAGIC 1. Read files from cloud storage using DataStreamReader API
# MAGIC 1. Transform the dataframe to add the following columns
# MAGIC     -   file path: Cloud file path
# MAGIC     -   ingestion date: Current Timestamp
# MAGIC 1. Write the transformed data stream to Delta Lake Table

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Read files using DataStreamReader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType

customers_schema = StructType(fields=[StructField("customer_id", IntegerType()),
                                     StructField("customer_name", StringType()),
                                     StructField("date_of_birth", DateType()),
                                     StructField("telephone", StringType()),
                                     StructField("email", StringType()),
                                     StructField("member_since", DateType()),
                                     StructField("created_timestamp", TimestampType())
                                    ]
                              )

# COMMAND ----------

customers_df = (
                    spark.readStream
                         .format("json")
                         .schema(customers_schema)
                         .load("/Volumes/gizmobox/landing/operational_data/customers_stream/")
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
                        .option("checkpointLocation", "/Volumes/gizmobox/landing/operational_data/customers_stream/_checkpoint_stream")
                        .toTable("gizmobox.bronze.customers_stream")
)

# COMMAND ----------

streaming_query.stop()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gizmobox.bronze.customers_stream;