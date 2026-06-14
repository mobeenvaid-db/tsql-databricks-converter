# Results — tsql-databricks-converter

Outcome of running the accelerator end to end on a real Microsoft SQL Server export, run fresh from raw T-SQL with parameters only.

## Headline

**90.5% of all objects converted and binding-validated automatically**,
on a clean run from raw input. The remaining objects (9.5%) are emitted to a classified
manual-review queue, concentrated in complex stored procedures and cross-database views.

## Portability — proven across clouds

The same code ran end to end on two different clouds with **only job parameters changing**,
no code edits:

- **AWS** — initial build + staged 25/50/75/100% rollout.
- **Azure** — full fresh run into a user-owned catalog on a Medium serverless warehouse.
  This is the run the numbers above come from.

Catalog, schema, warehouse, source dialect, scratch catalog, model tiers, and fraction are
all parameters with neutral defaults. The accelerator is cloud- and tenant-agnostic.

## What the conversion uses (sqlglot + LLM hybrid)

- **sqlglot** (deterministic, free): tables, views, schemas — the mechanical bulk.
- **Tiered LLM** (`ai_query`, Haiku → Sonnet → Opus by complexity): procedures, procedural
  functions, and anything sqlglot cannot fully transpile.
- On this run: ~40% objects via sqlglot, ~60% via the LLM (the split shifts as sqlglot
  misses route to the model).

## Bugs found and fixed (surfaced by cross-environment testing)

The first true end-to-end runs on fresh workspaces exposed four real defects the original
re-staged testing had masked. All are fixed in this package:

1. **sqlglot bracket-leak not routed to the LLM** — sqlglot emitted partially-transpiled
   output (e.g. 663 tables with leftover `[brackets]`) and the pipeline accepted it. Fix:
   sanity-check sqlglot output; any leftover T-SQL brackets force the object to the LLM.
2. **Validation assumed the target catalog allows schema creation** — failed on a restricted
   `users` catalog (schema quota). Fix: a `scratch_catalog` parameter decouples the validation
   substrate from the output catalog.
3. **Standalone task runs crashed** on `dbutils.jobs.taskValues.get` for a task absent from the
   run. Fix: try/except fallback to the latest run.
4. **Column-name special characters** — 186 tables failed `DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES`.
   Fix: a deterministic normalize rule adds `delta.columnMapping.mode='name'` when needed. This
   alone lifted tables 89% → 97% at zero token cost.

## Methodology learnings

- **Conversion-time rules beat post-hoc repair for systematic issues.** Six Opus repair
  iterations moved procedures +1pp; one re-convert with explicit SQL-scripting rules (hoist
  `DECLARE`s to the top of the block, `MERGE INTO`, terminate every statement) moved them +9pp.
  The DECLARE-placement error class went from 46 to 1.
- **Deterministic normalize fixes are the cheapest wins** — the column-mapping rule added
  +3.7pp overall with no LLM calls. Prefer code over AI for mechanical, high-volume issues.
- **Binding validation (not parse-only) is the authoritative gate** — objects can parse locally
  yet fail to bind on missing/cross-catalog references.

## Manual-review queue (488 objects)

The remaining failures are written to `<catalog>.<schema>.manual_review`, classified so they are
an actionable triage list rather than an opaque "fail" count:

| Error class | Count | Mostly | Nature |
|-------------|------:|--------|--------|
| parse_other        | 222 | views (cross-db `[db]..[table]` refs), procedures | mixed; some systematic |
| other              | 88  | tables (default-value types, unsupported datatypes) | mixed |
| statement_termination | 78 | procedures | scripting edge cases |
| merge_syntax       | 47  | procedures | T-SQL MERGE variants |
| missing_dependency | 19  | all types | reference to an object outside the set |
| unclosed_block     | 19  | procedures | BEGIN/END balance |
| pyspark_syntax     | 9   | procedures | PySpark-target compile errors |
| unresolved_column  | 5   | functions/views | column resolution |
| declare_placement  | 1   | procedures | (essentially eliminated by the rule) |

The genuinely-hard residual is complex stored procedures (cursors, dynamic SQL building
schema-dependent queries) — the same class commercial T-SQL converters leave for a human.

## Headroom

With additional iterative repair the same object set previously reached **97.7%**, so the
gap above 90.5% is real but takes more passes. The highest-leverage next steps are conversion-time
prompt rules for the residual procedure buckets (statement termination, MERGE variants) and a
deterministic fix for the cross-database `[db]..[table]` view references.

## Where to look

- Per-object status + errors: `<catalog>.<schema>.validation_results`
- Triage list: `<catalog>.<schema>.manual_review`
- Run history + per-type rates: `<catalog>.<schema>.run_summary`
- Reconciliation (every source object accounted for): `<catalog>.<schema>.recon_inventory`
- Live dashboard: the **T-SQL Migration Monitor** (built by `build_dashboard.py`)
