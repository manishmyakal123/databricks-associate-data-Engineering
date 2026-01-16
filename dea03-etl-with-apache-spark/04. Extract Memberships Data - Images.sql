-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Extract Data From the Memberships - Image Files
-- MAGIC 1. Query Memberships File using binaryFile Format
-- MAGIC 1. Create Memberships View in Bronze Schema

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Query Memberships Folder using binaryFile Format

-- COMMAND ----------

-- MAGIC %fs ls '/Volumes/gizmobox/landing/operational_data/memberships'

-- COMMAND ----------

SELECT path,modificationTime,length FROM binaryFile.`/Volumes/gizmobox/landing/operational_data/memberships/2024-10/*.png`

-- COMMAND ----------

SELECT * FROM binaryFile.`/Volumes/gizmobox/landing/operational_data/memberships/2024-10/*.png`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col
-- MAGIC df = (
-- MAGIC     spark.read.format("binaryFile")
-- MAGIC     .load("/Volumes/gizmobox/landing/operational_data/memberships/2024-10/*.png")
-- MAGIC     .select(
-- MAGIC         col("path"),
-- MAGIC         col("length"),
-- MAGIC         col("modificationTime"),
-- MAGIC         col("content")   # actual image bytes
-- MAGIC     )
-- MAGIC     
-- MAGIC )
-- MAGIC display(df.toPandas())

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
-- MAGIC     "gizmobox.landing.membership_images"
-- MAGIC )
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.table("gizmobox.landing.membership_images")
-- MAGIC display(df.toPandas())
-- MAGIC

-- COMMAND ----------

select path,modificationTime,length from gizmobox.landing.membership_images

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from PIL import Image
-- MAGIC import io
-- MAGIC
-- MAGIC row = spark.table("gizmobox.landing.membership_images") \
-- MAGIC            .select("path","modificationTime","length","content") \
-- MAGIC            .first()
-- MAGIC
-- MAGIC img = Image.open(io.BytesIO(row.content))
-- MAGIC display(img)
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE VIEW gizmobox.bronze.v_memberships
AS
SELECT  path,modificationTime,length FROM binaryFile.`/Volumes/gizmobox/landing/operational_data/memberships/*/*.png`;

-- COMMAND ----------

select * from gizmobox.bronze.v_memberships

-- COMMAND ----------

-- MAGIC %python
-- MAGIC rows = (
-- MAGIC     spark.table("gizmobox.landing.membership_images")
-- MAGIC     .select("path", "content")
-- MAGIC     .limit(3)
-- MAGIC     .collect()
-- MAGIC )
-- MAGIC
-- MAGIC from PIL import Image
-- MAGIC import io
-- MAGIC
-- MAGIC for r in rows:
-- MAGIC     print(r.path)
-- MAGIC     display(Image.open(io.BytesIO(r.content)))
-- MAGIC

-- COMMAND ----------

SELECT * FROM binaryFile.`/Volumes/gizmobox/landing/operational_data/memberships/2024-10/*.png`

-- COMMAND ----------

SELECT path,modificationTime,length FROM binaryFile.`/Volumes/gizmobox/landing/operational_data/memberships/*/*.png`

-- COMMAND ----------

SELECT * FROM binaryFile.`/Volumes/gizmobox/landing/operational_data/memberships/*/*.png`

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Create Memberships View in Bronze Schema

-- COMMAND ----------

CREATE OR REPLACE VIEW gizmobox.bronze.v_memberships
AS
SELECT path,modificationTime,length FROM binaryFile.`/Volumes/gizmobox/landing/operational_data/memberships/*/*.png`;

-- COMMAND ----------

CREATE OR REPLACE VIEW gizmobox.bronze.v_memberships
AS
SELECT * FROM binaryFile.`/Volumes/gizmobox/landing/operational_data/memberships/*/*.png`;

-- COMMAND ----------

SELECT * FROM gizmobox.bronze.v_memberships;

-- COMMAND ----------

SELECT path, modificationTime, Length FROM binaryFile.`/Volumes/gizmobox/landing/operational_data/memberships/*/*.png`;

-- COMMAND ----------

SELECT * FROM binaryFile.`/Volumes/gizmobox/landing/operational_data/memberships/*/*.png`;