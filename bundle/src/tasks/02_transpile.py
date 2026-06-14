# Databricks notebook source
# MAGIC %md # Task 2: transpile — sqlglot (deterministic, free, no rate limit)
# MAGIC sqlglot transpiles the mechanical bulk (tables, views, schemas) from the
# MAGIC source dialect to Databricks instantly with zero model calls. Procedural
# MAGIC objects and anything sqlglot cannot parse fall through to the AI convert task.

# COMMAND ----------
# MAGIC %pip install sqlglot
# MAGIC %restart_python

# COMMAND ----------
# MAGIC %run ../_common

# COMMAND ----------
sg_udf = make_sqlglot_udf(CATALOG, SOURCE_DIALECT)
sg = (spark.table(f"{FQ}.convert_requests").filter("NOT oversized")
      .withColumn("sg", sg_udf("object_type", "source_sql"))
      .selectExpr("object_id", "sg.code AS sqlglot_code", "sg.err AS sqlglot_err"))
sg.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}._sqlglot")
got = spark.table(f"{FQ}._sqlglot").filter("sqlglot_code IS NOT NULL").count()
tot = spark.table(f"{FQ}._sqlglot").count()
print(f"sqlglot transpiled {got}/{tot} eligible objects deterministically")
