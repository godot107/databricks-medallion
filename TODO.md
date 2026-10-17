# TODO

## First run (needs the workspace)
- [ ] Sign up for Free Edition; `databricks auth login`; `bundle deploy` + `bundle run` in dev
- [ ] Check the real reject rate on `samples.nyctaxi.trips`; tune `max_reject_rate`
- [ ] Confirm the Free Edition warehouse name matches the `warehouse_id` lookup
- [ ] Confirm on a real run (unverified offline): `{{job.run_id}}` arrives in wheel
      parameters; SET TAGS / SET ROW FILTER allowed in Free Edition; overwrite +
      `overwriteSchema` on a row-filtered gold table; information_schema.column_tags sees
      tags immediately; dashboard JSON renders (else rebuild in UI + `bundle generate`)
- [ ] Try RLS with `sql/rls_examples.sql` (restrict yourself to Manhattan)
- [ ] Push to GitHub (private first), add `prod` environment + secrets, watch the first deploy

## Next lessons
- [ ] Incremental bronze: Auto Loader from the `landing` volume instead of a full overwrite
- [ ] Same pipeline as Lakeflow Declarative Pipelines (`resources.pipelines`) with expectations; compare
- [ ] Grants as code: `grants` on the gold schema (read-only group)
- [ ] Column masks from tags (e.g. `mask=partial` on an ID column), same pattern as rls
- [ ] Dashboard: job health from `system.lakeflow` tables if Free Edition exposes them
- [ ] PR preview deploys: a `ci` target with a per-PR name prefix, destroyed on PR close
- [ ] Data quality checks in gold (row-count drift vs yesterday)

## AWS phase (only if/when moving off Free Edition)
- [ ] Terraform: S3 bucket + IAM role + UC storage credential (CloudBurn scan + TAGGING.md)
- [ ] Bundle (`engine: direct`): external location + per-environment catalogs
- [ ] CI auth via service principal + GitHub OIDC federation; separate dev/prod workspaces
