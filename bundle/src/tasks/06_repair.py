# Databricks notebook source
# MAGIC %md # Task 6: repair — error-driven, method-preserving LLM repair loop
# MAGIC For each genuinely-failing object, feed the EXACT Databricks error + prior code
# MAGIC to a stronger model, constrained to keep the object in its existing target method
# MAGIC (SQL view/table/proc/func vs PySpark). Re-validate the fix and update both
# MAGIC converted_objects and validation_results. Runs `repair_iterations` passes.

# COMMAND ----------
# MAGIC %run ../_common

# COMMAND ----------
assert WAREHOUSE_ID, "set warehouse_id"
CO = f"{FQ}.converted_objects"
names = set(r[0].lower() for r in wh_fetch(f"SELECT DISTINCT lower(object_name) FROM {FQ}.source_objects"))

def method_directive(otype, code):
    if otype == "VIEW":
        return ("Return a SINGLE Databricks SQL `CREATE OR REPLACE VIEW` statement. Do NOT convert "
                "to PySpark/Python and do NOT wrap in spark.sql(). Keep it a SQL view.")
    if otype == "TABLE":
        return "Return a SINGLE Databricks SQL `CREATE TABLE` statement. No Python, no trailing ALTER."
    if is_python(code):
        return ("This object is PySpark Python (def run(spark, params)). KEEP it PySpark Python; fix only "
                "what prevents compiling. Return Python only.")
    if otype == "PROCEDURE":
        return ("This object is a Databricks SQL CREATE PROCEDURE (LANGUAGE SQL). KEEP it SQL; return ONE "
                "`CREATE PROCEDURE` statement. SQL-scripting rules that cause most failures: (1) every "
                "DECLARE must be at the very START of its BEGIN block, before any executable statement - "
                "hoist all declarations up; (2) terminate EVERY statement with `;` and balance BEGIN/END; "
                "(3) T-SQL MERGE becomes Databricks `MERGE INTO target USING src ON ... WHEN MATCHED THEN "
                "UPDATE SET ... WHEN NOT MATCHED THEN INSERT (...) VALUES (...);`.")
    return "This object is a Databricks SQL CREATE FUNCTION (LANGUAGE SQL). KEEP it SQL; return ONE `CREATE FUNCTION` statement."

def ask(model, otype, code, error):
    usr = (f"Object type: {otype}\nMETHOD CONSTRAINT: {method_directive(otype, code)}\n\n"
           f"--- EXACT DATABRICKS ERROR ---\n{error}\n\n--- PRIOR OUTPUT ---\n{code}")
    body = {"messages": [{"role": "system", "content": RULEBOOK + "\n\n" + REPAIR_INSTRUCTION},
                         {"role": "user", "content": usr}], "max_tokens": 8000}
    try:
        r = requests.post(f"{_HOST}/serving-endpoints/{model}/invocations", headers=_H, timeout=180, json=body).json()
        return r["choices"][0]["message"]["content"].strip()
    except Exception:
        return None

def revalidate(otype, code):
    if otype in ("PROCEDURE", "FUNCTION") and is_python(code):
        try: compile(code, "<o>", "exec"); return "pass", ""
        except SyntaxError as e: return "fail", f"PYTHON_SYNTAX: {e}"[:300]
    sid, st, r = wh_submit(build_stmt(otype, code)); t0 = time.time()
    while st not in TERMINAL:
        if not sid or time.time()-t0 > 120: return "timeout", "cap"
        time.sleep(0.5); st, r = wh_poll(sid)
    return classify(st, wh_err(r), names), wh_err(r)

try:
    run_ts = dbutils.jobs.taskValues.get("validate", "run_ts", debugValue="")
except Exception:
    run_ts = ""   # standalone repair run (no validate task in this run)
run_ts = run_ts or wh_fetch(f"SELECT max(run_ts) FROM {FQ}.validation_results")[0][0]
for it in range(REPAIRS):
    fails = wh_fetch(f"""SELECT v.object_id, v.object_type, c.full_name, c.converted_code, v.valid_error
                         FROM {FQ}.validation_results v JOIN {CO} c USING (object_id)
                         WHERE v.run_ts='{run_ts}' AND v.valid_status IN ('fail','timeout')""")
    if not fails: print("no fails left"); break
    print(f"=== repair iteration {it+1}: {len(fails)} fails ===")
    tier_model = REPAIR_ENDPOINT["complex" if it > 0 else "medium"]
    def repair(row):
        oid, otype, fn, code, err = row
        fixed = strip_fences(ask(tier_model, otype, code, err or ""))
        if not fixed: return oid, None, None, None
        st, e = revalidate(otype, fixed); return oid, fixed, st, e
    fixed_ct = 0; updates = []
    with ThreadPoolExecutor(10) as ex:
        for oid, fixed, st, e in ex.map(repair, fails):
            if fixed:
                updates.append((oid, fixed, st, e))
                if st in ("pass", "unresolved_ext_dep", "sound_dep"): fixed_ct += 1
    print(f"  iteration {it+1}: fixed {fixed_ct}/{len(fails)}")
    # persist repaired code + updated statuses
    if updates:
        from pyspark.sql import Row
        upd_df = spark.createDataFrame([Row(object_id=o, converted_code=c, valid_status=s, valid_error=(e or "")[:2000]) for o, c, s, e in updates])
        upd_df.createOrReplaceTempView("_repairs")
        spark.sql(f"MERGE INTO {CO} t USING _repairs s ON t.object_id=s.object_id "
                  f"WHEN MATCHED THEN UPDATE SET t.converted_code=s.converted_code, t.attempt=t.attempt+1")
        spark.sql(f"MERGE INTO {FQ}.validation_results t USING _repairs s "
                  f"ON t.object_id=s.object_id AND t.run_ts='{run_ts}' "
                  f"WHEN MATCHED THEN UPDATE SET t.valid_status=s.valid_status, t.valid_error=s.valid_error")
print("repair complete")
