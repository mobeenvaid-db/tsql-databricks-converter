# Databricks notebook source
# MAGIC %md # Task 1: ingest — decode SSMS export + classify + build convert requests
# MAGIC Point `raw_sql_path` at a UC Volume holding the DDL-scripting export
# MAGIC (subfolders TABLE/ VIEW/ PROCEDURE/ FUNCTION/ SCHEMA/ SYNONYM). Decodes
# MAGIC UTF-16, parses metadata, computes routing signals (target form + complexity
# MAGIC tier), and builds per-object conversion prompts. No local tooling required.

# COMMAND ----------
# MAGIC %run ../_common

# COMMAND ----------
assert RAW_SQL_PATH, "set job parameter raw_sql_path to the UC Volume with the SQL export"
files = (spark.read.format("binaryFile").option("pathGlobFilter", "*.sql")
         .option("recursiveFileLookup", "true").load(RAW_SQL_PATH))
ing_udf = F.udf(_ingest_row, _INGEST_SCHEMA)
src = (files.withColumn("r", ing_udf(F.col("path"), F.col("content")))
       .select("r.*").withColumn("ingested_at", F.current_timestamp()))
src.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.source_objects")
n = spark.table(f"{FQ}.source_objects").count()
print(f"ingested {n} objects from {RAW_SQL_PATH}")

# build convert_requests: oversized flag, model endpoint, per-object prompt
bp = F.udf(lambda tf, sc, ob, sql: (build_prompt(tf, CATALOG, sc or "dbo", ob or "obj", sql or "")
                                    if tf else None), T.StringType())
work = (spark.table(f"{FQ}.source_objects")
        .withColumn("oversized", F.col("source_bytes") > F.lit(MAX_BYTES))
        .withColumn("model_endpoint", F.element_at(
            F.create_map([F.lit(x) for kv in ENDPOINTS.items() for x in kv]), F.col("complexity_tier")))
        .withColumn("prompt", bp("target_form", "schema_name", "object_name", "source_sql")))
(work.repartition(CONV_PARTS).write.mode("overwrite").option("overwriteSchema", "true")
     .saveAsTable(f"{FQ}.convert_requests"))
print("convert_requests:", spark.table(f"{FQ}.convert_requests").count(),
      "| oversized:", spark.table(f"{FQ}.convert_requests").filter("oversized").count())
display(spark.sql(f"SELECT object_type, target_form, complexity_tier, count(*) n "
                  f"FROM {FQ}.source_objects GROUP BY ALL ORDER BY 1,3"))
