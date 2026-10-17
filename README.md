# databricks-medallion

**The question:** how do you manage a Databricks data pipeline the way software teams
manage code: written down in files, reviewed in pull requests, and deployed by a robot
instead of by clicking around the web UI?

**The answer in this repo:** a *bundle* (a few YAML files) describes everything the
pipeline needs: the storage layout in Unity Catalog, the job, its three steps and its
schedule. GitHub Actions tests every change and, when it merges to `main`, deploys the
bundle and runs the job once to prove it works.

It runs on **Databricks Free Edition** (free, no cloud account needed), using the NYC
taxi sample data that every workspace has (`samples.nyctaxi.trips`).

## Plain-language glossary

| Term | What it means here |
|---|---|
| **Medallion architecture** | Store data in three quality tiers. **Bronze** = exactly what arrived (so you can always replay it). **Silver** = cleaned and checked. **Gold** = summarised for people to use. |
| **Unity Catalog** | Databricks' filing system and permissions: `catalog.schema.table`, e.g. `workspace.gold.daily_zip_summary`. |
| **Schema** | A folder of tables. This project makes one per tier: `bronze`, `silver`, `gold`. |
| **Volume** | A folder for files (CSV, JSON...) rather than tables. `bronze.landing` is where raw files would land. |
| **Job** | A scheduled list of tasks. Here: bronze, then silver, then gold, daily at 06:00. |
| **Bundle** | The YAML description of all the above. Officially *Declarative Automation Bundles* (formerly *Databricks Asset Bundles*, "DABs"). `databricks bundle deploy` makes the workspace match the files. |
| **Target** | An environment: `dev` (your sandbox) and `prod` (what the robot deploys). |
| **CI/CD** | *Continuous integration*: test every change automatically. *Continuous deployment*: ship it automatically once it passes. |

## What the pipeline does

```
samples.nyctaxi.trips ──► bronze.trips ──► silver.trips ──────────► gold.daily_zip_summary
   (sample data)          raw + lineage    valid, de-duplicated     trips / revenue per day per ZIP
                                           silver.trips_rejected
                                           (bad rows, kept for review)
```

- **Bronze** copies every row and stamps when and where it came from.
- **Silver** drops impossible trips (drop-off before pick-up, zero distance, non-positive
  fare, missing ZIP), removes duplicates and adds trip length in minutes. It keeps rejected
  rows in their own table, and acts as a **quality gate**: if more than 5% of rows are bad,
  the job fails and gold is *not* updated, so a bad batch never reaches the people using it.
- **Gold** summarises trips and revenue per day and pick-up ZIP code.

Every step overwrites its own table, so rerunning any step gives the same answer.

## Where everything lives

```
databricks.yml               bundle: name, variables, dev/prod targets, wheel build
resources/schemas.yml        Unity Catalog: bronze/silver/gold schemas + landing volume
resources/medallion_job.yml  the job: 3 tasks, dependencies, schedule, serverless env
src/medallion/transforms.py  the data logic (pure functions, tested locally)
src/medallion/jobs.py        entry points the job calls: read table → transform → write
tests/                       unit tests that run Spark on your laptop / in GitHub
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
databricks bundle deploy            # creates dev_<you>_bronze/silver/gold + the job
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
- **Real separation.** Paid setups usually have a separate workspace (or catalog) per
  environment. Here dev and prod share one workspace, kept apart by names.

See `TODO.md` for the AWS phase.
