# Databricks notebook source
# MAGIC %md # Task 3: convert — tiered LLM (ai_query) for sqlglot misses + procedures
# MAGIC Only objects sqlglot did not transpile (procedures, procedural functions,
# MAGIC parse failures) go to the model, routed by complexity tier to the cheapest
# MAGIC capable endpoint. sqlglot output + AI output are merged into converted_objects.

# COMMAND ----------
# MAGIC %run ../_common

# COMMAND ----------
# Output token cap per tier. Large/complex procedures (long MERGE column lists, wide
# INSERT ... VALUES, big CASE ladders) routinely exceed the old caps and got truncated
# mid-statement, which then failed validation as a parse error. Sized generously to the
# output limits of the tier models (Opus/Sonnet 4.x).
MAXTOK = {"trivial": 4096, "simple": 8192, "medium": 16000, "complex": 32000}
has_sg = spark.catalog.tableExists(f"{FQ}._sqlglot")
sg_ok = f"SELECT object_id FROM {FQ}._sqlglot WHERE sqlglot_code IS NOT NULL" if has_sg else None
spark.sql(f"""
  CREATE OR REPLACE TABLE {FQ}._needs_ai AS
  SELECT * FROM {FQ}.convert_requests r
  WHERE NOT r.oversized AND r.prompt NOT LIKE 'ERROR_BUILDING_PROMPT%'
    AND ({'r.object_id NOT IN (' + sg_ok + ')' if sg_ok else 'TRUE'})""")
print("objects needing AI:", spark.table(f"{FQ}._needs_ai").count())

def convert_tier(tier):
    cnt = spark.table(f"{FQ}._needs_ai").filter(f"complexity_tier='{tier}'").count()
    if cnt == 0: return None
    t0 = time.time(); out = f"{FQ}._conv_{tier}"
    run_ai_batch(f"{FQ}._needs_ai", out, ENDPOINTS[tier], f"complexity_tier='{tier}'", max_tokens=MAXTOK.get(tier, 8192))
    print(f"  tier={tier:8s} rows={cnt:5d} ({time.time()-t0:.0f}s)"); return out

with ThreadPoolExecutor(max_workers=len(ENDPOINTS)) as ex:
    parts = [p for p in ex.map(convert_tier, list(ENDPOINTS.keys())) if p]

ai_sel = (" UNION ALL ".join(f"SELECT object_id, model_endpoint, resp.result AS raw_code, "
          f"resp.errorMessage AS convert_error FROM {p}" for p in parts) if parts else
          "SELECT CAST(NULL AS STRING) object_id, CAST(NULL AS STRING) model_endpoint, "
          "CAST(NULL AS STRING) raw_code, CAST(NULL AS STRING) convert_error WHERE 1=0")
sg_join = f"LEFT JOIN {FQ}._sqlglot sg USING (object_id)" if has_sg else ""
sg_code = "sg.sqlglot_code" if has_sg else "CAST(NULL AS STRING)"
spark.sql(f"""
  CREATE OR REPLACE TABLE {FQ}.converted_objects AS
  WITH ai AS ({ai_sel})
  SELECT r.object_id, r.object_type, r.schema_name, r.object_name, r.full_name,
         r.target_form, r.complexity_tier, r.source_bytes, r.oversized,
         CASE WHEN {sg_code} IS NOT NULL THEN 'sqlglot' WHEN ai.raw_code IS NOT NULL THEN 'ai' END AS method,
         ai.model_endpoint, ai.convert_error, COALESCE({sg_code}, ai.raw_code) AS raw_code,
         1 AS attempt, current_timestamp() AS converted_at
  FROM {FQ}.convert_requests r {sg_join} LEFT JOIN ai USING (object_id)""")
(spark.table(f"{FQ}.converted_objects").withColumn("converted_code", strip_fences_udf("raw_code"))
   .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.converted_objects"))
bym = {r.method: r.n for r in spark.sql(f"SELECT method, count(*) n FROM {FQ}.converted_objects GROUP BY method").collect()}
print("converted:", spark.table(f"{FQ}.converted_objects").count(), "| by method:", bym)
