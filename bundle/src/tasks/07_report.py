# Databricks notebook source
# MAGIC %md # Task 7: report — run summary + reconciliation
# MAGIC Writes `run_summary` (per-type + overall success %, timestamps) and
# MAGIC `recon_inventory` (every source object accounted for: converted? validated?).
# MAGIC The monitoring dashboard reads these; refreshing this task refreshes the dashboard.

# COMMAND ----------
# MAGIC %run ../_common

# COMMAND ----------
try:
    run_ts = dbutils.jobs.taskValues.get("validate", "run_ts", debugValue="")
except Exception:
    run_ts = ""   # standalone report run (no validate task in this run)
run_ts = run_ts or spark.sql(f"SELECT max(run_ts) r FROM {FQ}.validation_results").collect()[0]["r"]
print("reporting on run_ts:", run_ts)

# run_summary (append-only history of every run, for trend + staged-rollout charts)
spark.sql(f"""
  CREATE TABLE IF NOT EXISTS {FQ}.run_summary (
    run_ts STRING, fraction DOUBLE, object_type STRING, n BIGINT, success BIGINT,
    pct_success DOUBLE, n_pass BIGINT, n_sound BIGINT, n_fail BIGINT, n_timeout BIGINT,
    reported_at TIMESTAMP)""")
spark.sql(f"DELETE FROM {FQ}.run_summary WHERE run_ts='{run_ts}'")
spark.sql(f"""
  INSERT INTO {FQ}.run_summary
  SELECT run_ts, fraction, object_type, count(*) n,
         sum(CASE WHEN valid_status IN ('pass','unresolved_ext_dep','sound_dep') THEN 1 ELSE 0 END) success,
         round(100.0*sum(CASE WHEN valid_status IN ('pass','unresolved_ext_dep','sound_dep') THEN 1 ELSE 0 END)/count(*),1) pct_success,
         sum(CASE WHEN valid_status='pass' THEN 1 ELSE 0 END) n_pass,
         sum(CASE WHEN valid_status IN ('unresolved_ext_dep','sound_dep') THEN 1 ELSE 0 END) n_sound,
         sum(CASE WHEN valid_status='fail' THEN 1 ELSE 0 END) n_fail,
         sum(CASE WHEN valid_status='timeout' THEN 1 ELSE 0 END) n_timeout,
         current_timestamp()
  FROM {FQ}.validation_results WHERE run_ts='{run_ts}' GROUP BY run_ts, fraction, object_type""")

# reconciliation inventory: source -> converted -> validated, per type
spark.sql(f"""
  CREATE OR REPLACE TABLE {FQ}.recon_inventory AS
  WITH s AS (SELECT object_type, count(*) source_n FROM {FQ}.source_objects GROUP BY 1),
       c AS (SELECT object_type, count(*) converted_n FROM {FQ}.converted_objects WHERE converted_code IS NOT NULL GROUP BY 1),
       v AS (SELECT object_type, count(*) validated_n,
                    sum(CASE WHEN valid_status IN ('pass','unresolved_ext_dep','sound_dep') THEN 1 ELSE 0 END) success_n
             FROM {FQ}.validation_results WHERE run_ts='{run_ts}' GROUP BY 1)
  SELECT s.object_type, s.source_n, COALESCE(c.converted_n,0) converted_n,
         COALESCE(v.validated_n,0) validated_n, COALESCE(v.success_n,0) success_n
  FROM s LEFT JOIN c USING (object_type) LEFT JOIN v USING (object_type) ORDER BY 1""")

# classified manual-review queue: every remaining failure, bucketed by error class,
# so the residual is an actionable triage list rather than an opaque "fail" count.
spark.sql(f"""
  CREATE OR REPLACE TABLE {FQ}.manual_review AS
  SELECT v.object_type, v.full_name, c.method,
    CASE WHEN v.valid_error LIKE '%ONLY_AT_BEGINNING%' THEN 'declare_placement'
         WHEN v.valid_error LIKE '%MATCHED%' THEN 'merge_syntax'
         WHEN v.valid_error LIKE '%DELTA_INVALID_CHARACTERS%' THEN 'column_name_chars'
         WHEN v.valid_error LIKE '%missing%' THEN 'statement_termination'
         WHEN v.valid_error LIKE '%end of input%' THEN 'unclosed_block'
         WHEN v.valid_error LIKE '%PYTHON_SYNTAX%' THEN 'pyspark_syntax'
         WHEN v.valid_error LIKE '%PARSE_SYNTAX%' THEN 'parse_other'
         WHEN v.valid_error LIKE '%UNRESOLVED_COLUMN%' THEN 'unresolved_column'
         WHEN v.valid_error LIKE '%NOT_FOUND%' THEN 'missing_dependency'
         WHEN v.valid_status='timeout' THEN 'timeout'
         ELSE 'other' END AS error_class,
    v.valid_status, substr(regexp_replace(v.valid_error,'[\\n]',' '),1,1000) AS error_detail
  FROM {FQ}.validation_results v JOIN {FQ}.converted_objects c USING(object_id)
  WHERE v.run_ts='{run_ts}' AND v.valid_status IN ('fail','timeout')""")
print(f"manual_review queue: {spark.table(f'{FQ}.manual_review').count()} objects")

print("=== run_summary ===")
spark.sql(f"SELECT * FROM {FQ}.run_summary WHERE run_ts='{run_ts}' ORDER BY object_type").show(truncate=False)
print("=== recon_inventory ===")
spark.sql(f"SELECT * FROM {FQ}.recon_inventory").show(truncate=False)
ov = spark.sql(f"""SELECT round(100.0*sum(success)/sum(n),1) pct, sum(success) success, sum(n) n
                   FROM {FQ}.run_summary WHERE run_ts='{run_ts}'""").collect()[0]
print(f"OVERALL: {ov['success']}/{ov['n']} = {ov['pct']}% converted + validated")
