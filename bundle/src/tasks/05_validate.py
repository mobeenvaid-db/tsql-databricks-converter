# Databricks notebook source
# MAGIC %md # Task 5: validate — binding validation into isolated scratch schemas
# MAGIC Deploys converted objects into `{catalog}.{scratch_prefix}{schema}` scratch
# MAGIC schemas (never touches the real target), in dependency order: tables first
# MAGIC (binding substrate), then views via EXPLAIN, functions/procedures via CREATE,
# MAGIC PySpark objects via compile(). Fans out to a large multi-cluster SQL warehouse.
# MAGIC `fraction` enables a staged rollout (0.25/0.5/0.75/1.0). Writes validation_results.

# COMMAND ----------
# MAGIC %run ../_common

# COMMAND ----------
assert WAREHOUSE_ID, "set job parameter warehouse_id to a large multi-cluster SQL warehouse"
import datetime
run_ts = datetime.datetime.utcnow().isoformat()
CO = f"{FQ}.converted_objects"

# source object names (for ext-dep vs in-set classification)
names = set(r[0].lower() for r in wh_fetch(f"SELECT DISTINCT lower(object_name) FROM {FQ}.source_objects"))

# ensure scratch schemas for every referenced schema (real identifiers only)
ref = re.compile(rf"(?i)`?{re.escape(CODE_CATALOG)}`?\s*\.\s*`?([A-Za-z0-9_ ]+?)`?\s*\.")
ident = re.compile(r"^[A-Za-z0-9_]+$"); schemas = set()
for (c,) in wh_fetch(f"SELECT converted_code FROM {CO} WHERE converted_code IS NOT NULL"):
    for m in ref.finditer(c or ""):
        s = m.group(1).strip()
        if ident.match(s): schemas.add(s)
print(f"ensuring {len(schemas)} scratch schemas in {SCRATCH_CATALOG}")
with ThreadPoolExecutor(16) as ex:
    list(ex.map(lambda s: wh_fetch(f"CREATE SCHEMA IF NOT EXISTS {SCRATCH_CATALOG}.`{SCRATCH_PREFIX}{s}`"), sorted(schemas)))

def fetch(otype):
    ids = [r[0] for r in wh_fetch(f"SELECT object_id FROM {CO} WHERE object_type='{otype}' "
                                  f"AND NOT oversized AND converted_code IS NOT NULL ORDER BY object_id")]
    if FRACTION < 1.0: ids = ids[:max(1, int(len(ids)*FRACTION))]
    rows = []
    for j in range(0, len(ids), 50):
        inlist = ",".join("'"+x+"'" for x in ids[j:j+50])
        rows += wh_fetch(f"SELECT object_id, full_name, object_name, converted_code FROM {CO} WHERE object_id IN ({inlist})")
    return rows

ALL = {}
for otype in DEPLOY_ORDER:
    rows = fetch(otype)
    if not rows: continue
    print(f"=== {otype}: {len(rows)} (fraction={FRACTION}) ===")
    if otype in ("PROCEDURE", "FUNCTION"):
        py = [r for r in rows if is_python(r[3])]; sq = [r for r in rows if not is_python(r[3])]
        res = validate_python(py); res.update(validate_batch(otype, sq, names))
    else:
        res = validate_batch(otype, rows, names)
    if otype == "TABLE":  # gentle retry for transient executor load-shedding
        retry = [r for r in rows if res[r[0]][0] in ("fail", "timeout") and "rejected from java.util.concurrent" in (res[r[0]][1] or "")]
        if retry: res.update(validate_batch(otype, retry, names, sub_workers=8, poll_workers=8, max_inflight=60))
    ALL[otype] = (res, rows)

# write validation_results
from pyspark.sql import Row
recs = []
for otype, (res, rows) in ALL.items():
    rm = {r[0]: r for r in rows}
    for oid, (st, er) in res.items():
        r = rm.get(oid)
        recs.append(Row(run_ts=run_ts, fraction=FRACTION, object_id=oid, object_type=otype,
                        full_name=(r[1] if r else None), valid_status=st, valid_error=(er or "")[:2000]))
if recs:
    spark.createDataFrame(recs).write.mode("append").option("mergeSchema", "true").saveAsTable(f"{FQ}.validation_results")
    print(f"wrote {len(recs)} rows to {FQ}.validation_results")

summary = spark.sql(f"""
  SELECT object_type, count(*) n,
         sum(CASE WHEN valid_status IN ('pass','unresolved_ext_dep','sound_dep') THEN 1 ELSE 0 END) success,
         round(100.0*sum(CASE WHEN valid_status IN ('pass','unresolved_ext_dep','sound_dep') THEN 1 ELSE 0 END)/count(*),1) pct
  FROM {FQ}.validation_results WHERE run_ts='{run_ts}' GROUP BY 1 ORDER BY 1""")
summary.show(truncate=False)
ov = spark.sql(f"""SELECT round(100.0*sum(CASE WHEN valid_status IN ('pass','unresolved_ext_dep','sound_dep') THEN 1 ELSE 0 END)/count(*),1) pct, count(*) n
                   FROM {FQ}.validation_results WHERE run_ts='{run_ts}'""").collect()[0]
print(f"OVERALL fraction={FRACTION}: {ov['pct']}% over {ov['n']} objects")
dbutils.jobs.taskValues.set("run_ts", run_ts)
dbutils.notebook.exit(json.dumps({"run_ts": run_ts, "fraction": FRACTION, "overall_pct": float(ov["pct"]), "n": int(ov["n"])}))
