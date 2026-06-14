#!/usr/bin/env python3
"""Generate the migration-monitor Lakeview dashboard JSON for the bundle.
Generic: pass --catalog/--schema for your org. Reads run_summary (trend +
staged rollout), recon_inventory (funnel), validation_results (failures/review).

  python3 build_dashboard.py --catalog <catalog> --schema <schema> --profile <profile> --warehouse <id> --create
"""
import sys, json, subprocess, argparse
from pathlib import Path
SKILL = Path.home() / ".vibe/marketplace/plugins/fe-databricks-tools/skills/databricks-lakeview-dashboard/resources"
sys.path.insert(0, str(SKILL))
from lakeview_builder import LakeviewDashboard  # noqa: E402
GREEN, AMBER, RED, GREY, BLUE = "#00A972", "#FFAB00", "#FF3621", "#919191", "#8BCAE7"

def build(cat, sch):
    fq = f"{cat}.{sch}"
    vr, rs, ri = f"{fq}.validation_results", f"{fq}.run_summary", f"{fq}.recon_inventory"
    latest = (f"(SELECT * FROM {vr} WHERE run_ts=(SELECT max(run_ts) FROM {vr} "
              f"WHERE fraction=(SELECT max(fraction) FROM {vr})))")
    d = LakeviewDashboard("T-SQL → Databricks Migration Monitor")
    d.add_dataset("kpi", "KPIs", f"""
        SELECT count(*) total,
               sum(CASE WHEN valid_status IN ('pass','unresolved_ext_dep','sound_dep') THEN 1 ELSE 0 END) success,
               round(100.0*sum(CASE WHEN valid_status IN ('pass','unresolved_ext_dep','sound_dep') THEN 1 ELSE 0 END)/count(*),1) pct_success,
               sum(CASE WHEN valid_status IN ('fail','timeout') THEN 1 ELSE 0 END) needs_review
        FROM {latest} v""")
    d.add_dataset("funnel", "Reconciliation funnel", f"SELECT object_type, source_n, converted_n, validated_n, success_n FROM {ri} ORDER BY object_type")
    d.add_dataset("bytype", "Status by type", f"""
        SELECT object_type, CASE WHEN valid_status='pass' THEN '1 pass'
               WHEN valid_status IN ('unresolved_ext_dep','sound_dep') THEN '2 sound (dep)'
               WHEN valid_status='timeout' THEN '3 timeout' ELSE '4 fail' END status, count(*) n
        FROM {latest} v GROUP BY 1,2 ORDER BY 1,2""")
    d.add_dataset("rollout", "Staged rollout", f"""
        SELECT cast(fraction as string) fraction, round(100.0*sum(success)/sum(n),1) pct_success, sum(n) n
        FROM {rs} GROUP BY fraction ORDER BY fraction""")
    d.add_dataset("errclass", "Failure classes", f"""
        SELECT CASE WHEN valid_error LIKE '%PARSE_SYNTAX_ERROR%' THEN 'parse syntax'
                    WHEN valid_error LIKE '%PYTHON_SYNTAX%' THEN 'python syntax'
                    WHEN valid_error LIKE '%UNRESOLVED_COLUMN%' THEN 'unresolved column'
                    WHEN valid_error LIKE '%IDENTIFIER_TOO_MANY_NAME_PARTS%' THEN 'cross-db 3-part name'
                    WHEN valid_error LIKE '%UNRESOLVABLE_TABLE_VALUED_FUNCTION%' THEN 'TVF unresolved'
                    WHEN valid_status='timeout' THEN 'timeout (transient)' ELSE 'other' END errclass, count(*) n
        FROM {latest} v WHERE valid_status IN ('fail','timeout') GROUP BY 1""")
    d.add_dataset("review", "Needs review", f"""
        SELECT object_type, full_name, substr(regexp_replace(valid_error,'[\\n]',' '),1,220) error
        FROM {latest} v WHERE valid_status IN ('fail','timeout') ORDER BY object_type, full_name""")

    d.add_page("Migration Overview")
    d.add_counter("kpi", "total", "SUM", "Objects validated", {"x": 0, "y": 0, "width": 1, "height": 3})
    d.add_counter("kpi", "success", "SUM", "Successful", {"x": 1, "y": 0, "width": 1, "height": 3})
    d.add_counter("kpi", "pct_success", "SUM", "Success %", {"x": 2, "y": 0, "width": 1, "height": 3})
    d.add_counter("kpi", "needs_review", "SUM", "Needs review", {"x": 3, "y": 0, "width": 1, "height": 3})
    d.add_bar_chart("bytype", "object_type", "n", "SUM", "Status by object type",
                    {"x": 0, "y": 3, "width": 4, "height": 7}, color_field="status", colors=[GREEN, BLUE, AMBER, RED])
    d.add_bar_chart("rollout", "fraction", "pct_success", "SUM", "Staged rollout — success % by fraction",
                    {"x": 4, "y": 0, "width": 2, "height": 5}, colors=[GREEN])
    d.add_bar_chart("funnel", "object_type", "source_n", "SUM", "Source object inventory",
                    {"x": 4, "y": 5, "width": 2, "height": 5}, colors=[BLUE])

    d.add_page("Quality & Failures")
    d.add_bar_chart("errclass", "errclass", "n", "SUM", "Failure classes",
                    {"x": 0, "y": 0, "width": 3, "height": 7}, sort_descending=True, colors=[RED])
    d.add_table("review", [{"field": "object_type", "title": "Type", "type": "string"},
                           {"field": "full_name", "title": "Object", "type": "string"},
                           {"field": "error", "title": "Error", "type": "string"}],
                "Objects needing review", {"x": 3, "y": 0, "width": 3, "height": 7})
    return d

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", default="main")
    ap.add_argument("--schema", default="tsql_migration")
    ap.add_argument("--profile", default="DEFAULT")
    ap.add_argument("--warehouse", default="")
    ap.add_argument("--parent", default="/Users/${workspace.current_user.userName}")
    ap.add_argument("--create", action="store_true")
    a = ap.parse_args()
    ser = build(a.catalog, a.schema).to_json()
    out = Path(__file__).parent / "bundle/dashboard"; out.mkdir(parents=True, exist_ok=True)
    (out / "migration_monitor.lvdash.json").write_text(ser)
    print("wrote", out / "migration_monitor.lvdash.json")
    if a.create:
        payload = json.dumps({"display_name": "T-SQL Migration Monitor", "warehouse_id": a.warehouse,
                              "parent_path": a.parent, "serialized_dashboard": ser})
        r = subprocess.run(["databricks", "api", "post", "/api/2.0/lakeview/dashboards",
                            "--profile", a.profile, "--json", payload], capture_output=True, text=True)
        try: print("dashboard_id:", json.loads(r.stdout).get("dashboard_id"))
        except Exception: print(r.stdout[:300], r.stderr[:300])

if __name__ == "__main__":
    main()
