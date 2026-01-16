# Databricks notebook source
# MAGIC %md
# MAGIC ## Join Customer and Address
# MAGIC Join customer data with address data to create a customer_address table which contains the address of each customer on the same record

# COMMAND ----------

df_customers = spark.table('gizmobox.silver.py_customers')
display(df_customers)

# COMMAND ----------

df_addresses = spark.table('gizmobox.silver.py_addresses')
display(df_addresses)

# COMMAND ----------

df_customer_address = (
    df_customers.join(df_addresses, "customer_id", "inner")
    .select(
        "customer_id",
        "customer_name",
        "email",
        "date_of_birth",
        "member_since",
        "telephone",
        "shipping_address_line_1",
        "shipping_city",
        "shipping_state",
        "shipping_postcode",
        "billing_address_line_1",
        "billing_city",
        "billing_state",
        "billing_postcode"        
    )
)
display(df_customer_address)

# COMMAND ----------

df_customer_address.writeTo("gizmobox.gold.py_customer_address").createOrReplace()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gizmobox.gold.py_customer_address;

# COMMAND ----------

# MAGIC %md
# MAGIC