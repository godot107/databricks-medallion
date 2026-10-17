# databricks-medallion

CI/CD for a Databricks medallion pipeline: a bundle (Declarative Automation Bundles,
formerly DABs) defines UC schemas + volume, a cron-scheduled 4-task PySpark job and an
AI/BI monitoring dashboard; GitHub Actions
tests every push and deploys `prod` on merge to main. Targets **Databricks Free Edition**.

## Run

```bash
.venv/bin/pytest -q                                   # local Spark, needs Java 17+
databricks bundle schema > /tmp/s.json && .venv/bin/python tools/check_bundle.py /tmp/s.json
databricks bundle deploy && databricks bundle run medallion_daily   # dev target
cd notebooks && ../.venv/bin/python _build.py                      # rebuild + execute walkthrough
```

## Decisions / constraints

- Free Edition only for now (Willie, 2026-10-17): serverless compute, `workspace`
  catalog, no own S3 / storage credentials / external locations, no account console →
  CI auth is a PAT in the GitHub `prod` environment, not service-principal OIDC.
- Code ships as a wheel (`artifacts` → `python_wheel_task` entry points), the pattern
  from the default-python template; transforms are pure DataFrame functions so tests
  run on local Spark. pyspark pinned to 3.5.x to match serverless env version 2.
- Each layer overwrites its own table → idempotent reruns. Silver gates on reject rate
  (`max_reject_rate`, default 5%) before gold publishes.
- Dev and prod share one workspace; development mode prefixes names and pauses schedules.
- `tools/check_bundle.py` validates YAML offline against `databricks bundle schema`
  (full `bundle validate` needs a workspace login). CLI pinned v1.19.0.
- Reusable expressions in `common.py`; `transforms.py` only composes them. Silver rules
  are a function (`silver_rules()`) because Column objects need an active Spark session.
- Tags are code (`governance.py`), applied by the `governance` task after every load,
  because tables aren't bundle resources and overwrites can drop them. RLS = UC tags +
  classic row filters chosen by reading `information_schema.column_tags` for `rls=<dim>`;
  access map `ops.rls_access`; deployer always gets `*`. ABAC (`sql/abac_policy.sql`)
  needs account-level governed tags → not Free Edition.
- RLS only on gold: row filters apply to the job's own reads too, so filtering silver
  would silently shrink gold.
- Monitoring = `ops.pipeline_runs` (one row per task, failures included) + dashboard with
  `dataset_schema: ops`, so dataset SQL uses unqualified ops names (gold via the
  `v_gold_daily_borough` view). `embed_credentials: false` keeps RLS per viewer.
  Warehouse found by `lookup: warehouse: Serverless Starter Warehouse`.
