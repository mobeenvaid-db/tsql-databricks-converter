# tsql-databricks-converter

A portable Databricks accelerator that converts an entire Microsoft SQL Server (T-SQL)
codebase to Databricks SQL / PySpark, then binding-validates and repairs the output. It
runs as a multi-task Databricks Job and ships with a Lakeview monitoring dashboard. Nothing
is hardcoded to a customer, catalog, or workspace — everything is a job parameter.

## How it works

A 7-task DAG, each task a focused notebook that shares one library (`src/_common.py`):

| Task | What it does | Engine |
|------|--------------|--------|
| `ingest` | Decode the SSMS "Generate Scripts" export from a UC Volume (handles UTF-16), classify each object (target form + complexity tier), build per-object prompts | deterministic |
| `transpile` | Transpile the mechanical bulk (tables, views, schemas) to the Databricks dialect | **sqlglot** (free, instant) |
| `convert` | Convert what sqlglot cannot (procedures, procedural functions, parse failures), routed by complexity to the cheapest capable model | **tiered LLM** (`ai_query`: Haiku → Sonnet → Opus) |
| `normalize` | Deterministic Delta-compatibility fixes (IDENTITY→BIGINT, column-DEFAULT table feature, bare-NULL strip, UNIQUE strip, SQL SECURITY INVOKER) | deterministic |
| `validate` | Deploy into isolated scratch schemas and binding-validate: tables (CREATE), views (EXPLAIN), functions/procedures (CREATE), PySpark objects (compile). Fans out to a large multi-cluster SQL warehouse. `fraction` enables a staged rollout | deterministic |
| `repair` | Feed the exact Databricks error + prior code back to a stronger model, constrained to keep each object in its existing method (SQL vs PySpark), re-validate | **LLM** |
| `report` | Write `run_summary` + `recon_inventory` (every source object accounted for); the dashboard reads these | deterministic |

The sqlglot + LLM split is the point: sqlglot carries the deterministic majority for free,
the LLM is spent only where procedural T-SQL genuinely needs it.

## Quick start

1. Drop your SSMS "Generate Scripts" export into a UC Volume, with one subfolder per object
   type: `TABLE/ VIEW/ PROCEDURE/ FUNCTION/ SCHEMA/ SYNONYM`, one `.sql` file per object.
2. Edit `bundle/databricks.yml` — set the `dev` target's `workspace.host` and the variables
   (`catalog`, `schema`, `warehouse_id`, `raw_sql_path`). Use a **large, multi-cluster** SQL
   warehouse for `warehouse_id` (validation fans out hundreds of statements concurrently).
3. Deploy and run:
   ```bash
   databricks bundle deploy -t dev --profile <your-profile>
   databricks bundle run   tsql_migration -t dev --profile <your-profile>
   ```
4. Open the **T-SQL Migration Monitor** dashboard to track the funnel, per-type success, failure
   classes, and the review queue.

### Staged rollout
Run with `fraction=0.25`, then `0.5`, `0.75`, `1.0` to validate at increasing scale and watch
the success rate hold before committing the full set. Each run appends to `run_summary` so the
dashboard's "staged rollout" chart shows the progression.

### Re-running individual stages
Tasks are independent and idempotent. Re-run only `validate`+`repair` after tuning, or only
`report` to refresh the dashboard, by triggering those tasks on the job.

## Parameters (all job-level, neutral defaults)

| Parameter | Default | Notes |
|-----------|---------|-------|
| `catalog` | `main` | Target Unity Catalog |
| `schema` | `tsql_migration` | Schema for all artifacts |
| `warehouse_id` | — | Large multi-cluster SQL warehouse (required for validate/repair) |
| `raw_sql_path` | — | UC Volume with the export (required for ingest) |
| `source_dialect` | `tsql` | sqlglot read dialect |
| `fraction` | `1.0` | Fraction of objects to validate (staged rollout) |
| `repair_iterations` | `2` | Error-driven repair passes |
| `scratch_prefix` | `_migrate_scratch__` | Prefix for the isolated validation scratch schemas |
| `scratch_catalog` | = `catalog` | Catalog that hosts the validation scratch schemas. Set this if the target catalog cannot create schemas (for example a restricted `users` catalog) |
| `code_catalog` | = `catalog` | Only set differently when validating code generated against another catalog name |
| `model_trivial/simple/medium/complex` | Haiku/Sonnet/Sonnet/Opus | Per-tier model endpoints |

## Output tables (`{catalog}.{schema}`)

- `source_objects` — every parsed source object + routing signals
- `convert_requests` — prompts + tier + oversized flag
- `converted_objects` — final converted code per object, with `method` (sqlglot vs ai)
- `validation_results` — per-object binding status per run (`pass`/`sound_dep`/`unresolved_ext_dep`/`fail`/`timeout`)
- `run_summary`, `recon_inventory` — dashboard sources

Success = `pass` + `sound_dep` + `unresolved_ext_dep` (an object whose only unresolved
references are to objects outside the exported set is a sound conversion, not a failure).

## Components

- `bundle/` — the Databricks Asset Bundle (job DAG + dashboard).
- `bundle/src/_common.py` — shared config, prompts, transforms, validation engine.
- `bundle/src/tasks/` — the 7 task notebooks.
- `build_dashboard.py` — regenerate + deploy the Lakeview monitoring dashboard for your
  catalog/schema: `python3 build_dashboard.py --catalog <c> --schema <s> --profile <p> --create`.
  The dashboard reads `run_summary` / `recon_inventory` / `validation_results` and shows the
  funnel, per-type success, failure classes, the staged-rollout trend, and the review queue.

## Notes

- **`bundle deploy` Terraform GPG error** (`openpgp: key expired`): a known bug in older
  `databricks` CLI builds (HashiCorp's signing key expired). Fix with `brew upgrade databricks`
  (or your install method). The job/notebooks can also be deployed via the Jobs + Workspace
  APIs without Terraform if needed.
- **Validation warehouse sizing**: the validate task only autoscales a warehouse when statements
  queue. Use a genuinely large multi-cluster warehouse; a single small cluster is the throughput
  ceiling.

## Reference result

Validated end to end on a real 5,156-object SQL Server export, run fresh from raw T-SQL on
two clouds (AWS and Azure) with parameters only:

- **Automated (convert + validate + repair) on a clean run from raw input: 90.5%** —
  schemas 100%, tables 97.4%, functions 89.2%, views 83.6%, procedures 83.5%. The remaining
  9.5% are emitted to a classified manual-review queue (`manual_review` table), concentrated in
  complex stored procedures and cross-database views — the same class commercial converters
  leave for a human.
- With additional iterative repair the same set previously reached **97.7%**, so the headroom
  above 90.5% is real but takes more passes.

See `RESULTS.md` for the full breakdown, cross-cloud portability evidence, and the bug log.
