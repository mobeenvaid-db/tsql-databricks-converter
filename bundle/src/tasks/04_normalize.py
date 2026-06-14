# Databricks notebook source
# MAGIC %md # Task 4: normalize — deterministic Delta-compatibility fixes (no AI)
# MAGIC Mechanical incompatibilities every model output shares are cheaper and more
# MAGIC reliable to fix with code than with thousands of AI repair calls:
# MAGIC IDENTITY->BIGINT, column-DEFAULT table feature, bare-NULL strip, UNIQUE strip,
# MAGIC SQL SECURITY INVOKER on procedures, idempotent CREATE SCHEMA.

# COMMAND ----------
# MAGIC %run ../_common

# COMMAND ----------
norm_udf = F.udf(normalize_databricks_sql, T.StringType())
n = spark.table(f"{FQ}.converted_objects").count()
(spark.table(f"{FQ}.converted_objects")
   .withColumn("converted_code", norm_udf("converted_code", "object_type"))
   .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.converted_objects"))
print(f"normalized {n} objects (IDENTITY->BIGINT, DEFAULT feature, NULL/UNIQUE strip, proc security)")
