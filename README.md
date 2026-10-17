# databricks-medallion

**The question:** how do you manage a Databricks data pipeline the way software teams
manage code: written down in files, reviewed in pull requests, and deployed by a robot
instead of by clicking around the web UI?

**The answer in this repo:** a *bundle* (a few YAML files) describes everything the
pipeline needs: the storage layout in Unity Catalog, a PySpark job on a cron schedule,
and a monitoring dashboard. Shared transformation helpers keep each step short, and tags
on the data decide who can see which rows. GitHub Actions tests every change and, when
it merges to `main`, deploys the bundle and runs the job once to prove it works.

It runs on **Databricks Free Edition** (free, no cloud account needed), using the NYC
taxi sample data that every workspace has (`samples.nyctaxi.trips`).

## Plain-language glossary

| Term | What it means here |
|---|---|
| **Medallion architecture** | Store data in three quality tiers. **Bronze** = exactly what arrived (so you can always replay it). **Silver** = cleaned and checked. **Gold** = summarised for people to use. |
| **Unity Catalog** | Databricks' filing system and permissions: `catalog.schema.table`, e.g. `workspace.gold.daily_zip_summary`. |
| **Schema** | A folder of tables. This project makes one per tier: `bronze`, `silver`, `gold`. |
| **Volume** | A folder for files (CSV, JSON...) rather than tables. `bronze.landing` is where raw files would land. |
| **Job** | A scheduled list of tasks. Here: bronze → silver → gold → governance, on a cron schedule. |
| **Cron** | A compact way to write a schedule. Databricks uses *Quartz* cron, which has a seconds field first: `0 0 6 * * ?` = 06:00:00 every day; `0 0 */4 * * ?` = every 4 hours. |
| **Tag** | A `key = value` label on a schema, table or column, e.g. `sensitivity = restricted`. Searchable, and here it also switches on security. |
| **Row-level security (RLS)** | Two people run the same query on the same table and get different rows, depending on what they're allowed to see. |
| **Row filter** | The Unity Catalog feature that does RLS: a small SQL function attached to a table that returns TRUE for rows the current user may see. |
| **Dashboard** | A Databricks AI/BI page of charts. Its layout is a JSON file in this repo, so it is deployed and reviewed like code. |
| **Bundle** | The YAML description of all the above. Officially *Declarative Automation Bundles* (formerly *Databricks Asset Bundles*, "DABs"). `databricks bundle deploy` makes the workspace match the files. |
| **Target** | An environment: `dev` (your sandbox) and `prod` (what the robot deploys). |
| **CI/CD** | *Continuous integration*: test every change automatically. *Continuous deployment*: ship it automatically once it passes. |

## What the pipeline does

```
samples.nyctaxi.trips ─► bronze.trips ─► silver.trips ─────────► gold.daily_zip_summary ─► governance
   (sample data)         raw + lineage   valid, de-duplicated,   trips / revenue per day,    tags + row
                                         + borough               borough, ZIP (row-filtered) filters
                                         silver.trips_rejected
                                         (bad rows + reasons)
        every task ─► ops.pipeline_runs (rows in/out, reject rate, duration, status) ─► dashboard
```

- **Bronze** copies every row and stamps when, where from, and in which run it arrived.
- **Silver** checks six named rules (drop-off before pick-up, zero distance, non-positive
  fare, missing ZIP...), removes duplicates, and adds trip length and borough. Rejected rows
  keep the list of rules they broke. It's also a **quality gate**: if more than 5% of rows
  are bad, the job fails and gold is *not* updated, so a bad batch never reaches users.
- **Gold** summarises trips and revenue per day, borough and pick-up ZIP code.
- **Governance** applies tags and row-level security (see below).

Every step overwrites its own table, so rerunning any step gives the same answer.

## Scheduling (cron)

The job runs on `schedule_cron` (default `0 0 6 * * ?`, 06:00 Central) set in
`databricks.yml`. Change it per environment under `targets:`. Development mode pauses
the schedule so your sandbox never runs by itself; trigger it with `bundle run`.
Failures email whoever deployed the bundle.

## Shared transformation expressions

`src/medallion/common.py` holds helpers that know nothing about taxis, so the next
pipeline reuses them instead of copy-pasting:

| Helper | Does |
|---|---|
| `snake_case_columns`, `trim_strings`, `cast_columns` | tidy names, blank → NULL, enforce types |
| `add_audit_columns` / `drop_audit_columns` | lineage columns `_ingested_at`, `_source`, `_run_id` |
| `split_valid(df, rules)` | named rules → (valid, rejected with `_reject_reasons`); NULL counts as a failure |
| `dedupe_exact`, `dedupe_latest(keys, order_by)` | remove copies; or keep the newest row per key |
| `minutes_between`, `bucket(col, ranges)` | derived values, e.g. ZIP → borough |
| `summarize(by, sums, avgs)` | group-by with predictable column names |

`transforms.py` composes them per layer. Both are plain DataFrame-in, DataFrame-out
functions, so they're unit-tested on a laptop.

## Monitoring dashboard

Every task appends one row to `ops.pipeline_runs`, even when it fails, so the
**Medallion pipeline monitor** dashboard (deployed by the bundle) shows:

- *Pipeline health:* failed tasks, worst reject rate, average duration, the latest run
  task by task, rows written per day, reject rate over time.
- *Trips by borough:* revenue and trips from gold, **through the row filter**. The
  dashboard runs each viewer's queries as that viewer (`embed_credentials: false`), so
  each person sees only their boroughs.

To change the layout, edit it in the Databricks UI, then pull it back into git:
`databricks bundle generate dashboard --resource medallion_monitor --force`.

## Tags and row-level security

Tags are declared in `src/medallion/governance.py` and applied by the governance task:

- **Descriptive tags** on schemas, tables and columns: `layer`, `domain`, `sensitivity`,
  `certified`, `contains_pii`, `refresh`, `geo`, `metric`. Search for them in Catalog
  Explorer or `information_schema`.
- **The security tag `rls = <dimension>`** on a column (here gold's `pickup_borough`).
  The task finds every column with that tag in Unity Catalog and attaches the row filter
  `ops.rls_<dimension>` to it. Securing another table means adding one tag.
- **Who sees what** lives in the table `ops.rls_access(principal, dimension, value)`. The
  principal is a user email or a group name, and the value `*` means all rows. The person who
  deploys always gets `*`, so the pipeline can still read its own data.

`sql/rls_examples.sql` walks through restricting yourself to Manhattan and back.
`sql/abac_policy.sql` is the paid-account version: one *attribute-based access control*
policy on governed tags that covers every matching table automatically. Free Edition
can't create governed tags, since that needs the account console.

## Where everything lives

```
databricks.yml               bundle: variables (catalog, cron, warehouse), dev/prod targets, wheel build
resources/schemas.yml        Unity Catalog: bronze/silver/gold/ops schemas + landing volume
resources/medallion_job.yml  the job: 4 PySpark tasks, cron schedule, failure email, serverless env
resources/dashboard.yml      the monitoring dashboard resource
src/medallion/common.py      reusable transformation expressions
src/medallion/transforms.py  taxi-specific layers built from common.py
src/medallion/governance.py  tags + tag-driven row filters, as code
src/medallion/monitoring.py  per-task run metrics → ops.pipeline_runs
src/medallion/jobs.py        entry points the job calls: read table → transform → write
src/dashboards/              dashboard layout (JSON)
sql/                         RLS walkthrough + paid-account ABAC policy
tests/                       unit tests: Spark on your laptop / in GitHub, plus dashboard checks
tools/check_bundle.py        checks the YAML against the official schema, no login needed
.github/workflows/ci.yml     every push/PR: tests + bundle check + build
.github/workflows/deploy.yml merge to main: validate → deploy prod → run the job
notebooks/walkthrough.ipynb  the transforms step by step on a tiny example
```

## How a change flows

1. Branch, edit, `pytest` locally.
2. Open a PR → **ci** runs the tests and checks the bundle YAML. No Databricks secrets are
   needed, so this is safe for every PR.
3. Merge → **deploy** runs `bundle validate`, `bundle deploy -t prod`, then
   `bundle run -t prod medallion_daily`. A red run means the deploy or the data failed.

## Set it up

**1. Free Edition workspace.** Sign up at databricks.com (Free Edition). Copy the
workspace URL from the browser (`https://dbc-....cloud.databricks.com`).

**2. CLI on your machine.**

```bash
# install: https://docs.databricks.com/aws/en/dev-tools/cli/install
databricks auth login --host https://dbc-....cloud.databricks.com   # opens the browser
python -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
.venv/bin/pytest -q
```

**3. Your own dev copy.**

```bash
databricks bundle validate          # dev is the default target
databricks bundle deploy            # creates dev_<you>_bronze/silver/gold/ops, the job, the dashboard
databricks bundle run medallion_daily
databricks bundle summary           # links to everything it made
```

Development mode names things `[dev <you>] medallion-daily` / `dev_<you>_bronze` and
pauses the schedule, so experiments never touch prod. `databricks bundle destroy` removes it all.

**4. Let GitHub deploy prod.** In the repo: *Settings → Environments → New environment*
`prod`, then add two secrets:

- `DATABRICKS_HOST`: the workspace URL
- `DATABRICKS_TOKEN`: *User settings → Developer → Access tokens → Generate* (set an expiry)

Optionally add yourself as a required reviewer on `prod`, so every deploy waits for a click.

## Free Edition vs a paid workspace (AWS or Azure)

Everything here, including bundles, Unity Catalog, serverless jobs and GitHub Actions, works the
same on Databricks on AWS. Azure isn't needed. What Free Edition can't do:

- **Your own storage.** On AWS you'd add an S3 bucket, an IAM role, a UC *storage
  credential* (Terraform or the console) and an *external location* plus your own catalogs
  (bundles can declare those now, with `engine: direct`). Free Edition uses Databricks-managed storage.
- **Robot identity.** On a paid account, CI should log in as a *service principal* via
  GitHub OIDC (`DATABRICKS_AUTH_TYPE: github-oidc`, no stored secret). Free Edition has no
  account console for that, so CI uses a personal access token.
- **Governed tags / ABAC.** Account-level, so the tag-driven row filters here are applied
  by the job instead of by one policy.
- **Real separation.** Paid setups usually have a separate workspace (or catalog) per
  environment. Here dev and prod share one workspace, kept apart by names.

See `TODO.md` for the AWS phase.
