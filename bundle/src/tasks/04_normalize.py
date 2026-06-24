# Databricks notebook source
# MAGIC %md # Task 4: normalize — deterministic Delta-compatibility fixes (no AI)
# MAGIC Mechanical incompatibilities every model output shares are cheaper and more
# MAGIC reliable to fix with code than with thousands of AI repair calls:
# MAGIC IDENTITY->BIGINT, column-DEFAULT table feature, bare-NULL strip, UNIQUE strip,
# MAGIC SQL SECURITY INVOKER on procedures, idempotent CREATE SCHEMA.
# MAGIC
# MAGIC Also deterministically rewrites SQL Server system-versioned TEMPORAL tables into
# MAGIC Delta SCD Type 2 (table + companion `_scd2_apply` MERGE procedure), overriding the
# MAGIC lossy LLM output so history semantics are preserved instead of silently flattened.

# COMMAND ----------
# MAGIC %run ../_common

# COMMAND ----------
norm_udf = F.udf(normalize_databricks_sql, T.StringType())
n = spark.table(f"{FQ}.converted_objects").count()
(spark.table(f"{FQ}.converted_objects")
   .withColumn("converted_code", norm_udf("converted_code", "object_type"))
   .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.converted_objects"))
print(f"normalized {n} objects (IDENTITY->BIGINT, DEFAULT feature, NULL/UNIQUE strip, proc security)")

# COMMAND ----------
# MAGIC %md ## Temporal (system-versioned) tables -> Delta SCD Type 2 (deterministic override)

# COMMAND ----------
import hashlib, datetime
co = spark.table(f"{FQ}.converted_objects")
has_temporal_col = "sig_has_temporal" in spark.table(f"{FQ}.source_objects").columns
if not has_temporal_col:
    print("source_objects has no sig_has_temporal (re-ingest to enable temporal SCD2); skipping")
else:
    src = spark.table(f"{FQ}.source_objects").select("object_id", "source_sql", "sig_has_temporal")
    temporal = (co.filter("object_type = 'TABLE'").join(src, "object_id")
                  .filter("sig_has_temporal = true")
                  .select("object_id", "schema_name", "object_name", "full_name", "source_sql").collect())
    print(f"temporal tables detected: {len(temporal)}")

    tbl_updates = []   # (object_id, scd2_table_ddl)
    proc_rows = []     # new companion procedure objects
    skipped = []
    for r in temporal:
        meta = parse_temporal_table(r["source_sql"])
        if not meta:
            skipped.append(r["full_name"]); continue
        tbl_updates.append((r["object_id"], scd2_table_ddl(meta, CODE_CATALOG)))
        proc_code = scd2_apply_proc(meta, CODE_CATALOG)
        pname = f"{meta['table']}_scd2_apply"
        proc_rows.append({
            "object_id": hashlib.sha1(f"PROCEDURE/{meta['schema']}.{pname}".encode()).hexdigest()[:16],
            "object_type": "PROCEDURE", "schema_name": meta["schema"], "object_name": pname,
            "full_name": f"{meta['schema']}.{pname}", "target_form": "sql_scripting",
            "complexity_tier": "generated", "source_bytes": 0, "oversized": False,
            "method": "scd2_generator", "model_endpoint": None, "convert_error": None,
            "raw_code": proc_code, "attempt": 1, "converted_code": proc_code})
    if skipped: print("could not parse (left as-is):", skipped)

    # 1) override the temporal tables' converted_code with the SCD2 DDL
    if tbl_updates:
        upd = spark.createDataFrame(tbl_updates, "object_id string, scd2 string")
        upd.createOrReplaceTempView("_scd2_tbl")
        spark.sql(f"MERGE INTO {FQ}.converted_objects t USING _scd2_tbl s ON t.object_id = s.object_id "
                  f"WHEN MATCHED THEN UPDATE SET t.converted_code = s.scd2, t.method = 'scd2_generator', "
                  f"t.convert_error = NULL")
        print(f"rewrote {len(tbl_updates)} temporal tables to SCD2 Delta DDL")

    # 2) append the companion _scd2_apply procedures as first-class objects (they get validated)
    if proc_rows:
        cols = spark.table(f"{FQ}.converted_objects").columns
        now = datetime.datetime.utcnow()
        rows = []
        for p in proc_rows:
            p = dict(p); p["converted_at"] = now
            rows.append([p.get(c) for c in cols])
        # build with the live table schema so the append matches exactly
        new_df = spark.createDataFrame(rows, schema=spark.table(f"{FQ}.converted_objects").schema)
        existing = [x[0] for x in spark.sql(f"SELECT object_id FROM {FQ}.converted_objects").collect()]
        new_df = new_df.filter(~F.col("object_id").isin(existing))
        new_df.write.mode("append").option("mergeSchema", "true").saveAsTable(f"{FQ}.converted_objects")
        print(f"added {new_df.count()} companion _scd2_apply procedure objects")
    print("temporal -> SCD2 complete")
