# TODO

## First run (needs the workspace)
- [ ] Sign up for Free Edition; `databricks auth login`; `bundle deploy` + `bundle run` in dev
- [ ] Check the real reject rate on `samples.nyctaxi.trips`; tune `max_reject_rate`
- [ ] Push to GitHub (private first), add `prod` environment + secrets, watch the first deploy

## Next lessons
- [ ] Incremental bronze: Auto Loader from the `landing` volume instead of a full overwrite
- [ ] Same pipeline as Lakeflow Declarative Pipelines (`resources.pipelines`) with expectations; compare
- [ ] Grants as code: `grants` on the gold schema (read-only group)
- [ ] PR preview deploys: a `ci` target with a per-PR name prefix, destroyed on PR close
- [ ] Data quality checks in gold (row-count drift vs yesterday)

## AWS phase (only if/when moving off Free Edition)
- [ ] Terraform: S3 bucket + IAM role + UC storage credential (CloudBurn scan + TAGGING.md)
- [ ] Bundle (`engine: direct`): external location + per-environment catalogs
- [ ] CI auth via service principal + GitHub OIDC federation; separate dev/prod workspaces
