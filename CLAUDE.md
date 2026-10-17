# databricks-medallion

CI/CD for a Databricks medallion pipeline: a bundle (Declarative Automation Bundles,
formerly DABs) defines UC schemas + volume + a 3-task serverless job; GitHub Actions
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
