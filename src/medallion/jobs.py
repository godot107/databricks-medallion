"""Job entry points (wheel console scripts) called by the bundle's job tasks.

Each layer reads the previous layer's table and overwrites its own, so a rerun
of any task gives the same result (idempotent). Every task appends its metrics
to ops.pipeline_runs for the monitoring dashboard.
"""
import argparse

from pyspark.sql import SparkSession

from medallion import governance, monitoring, transforms


def _args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    for layer in ("bronze", "silver", "gold", "ops"):
        p.add_argument(f"--{layer}_schema", required=True)
    p.add_argument("--run_id", default="manual")
    p.add_argument("--source_table")
    p.add_argument("--max_reject_rate", type=float, default=0.05)
    return p.parse_args()


def _ctx():
    a = _args()
    spark = SparkSession.builder.getOrCreate()
    table = lambda layer, name: f"{a.catalog}.{getattr(a, layer + '_schema')}.{name}"
    return a, spark, table


def _write(df, table: str) -> int:
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
    return df.sparkSession.table(table).count()


def run_bronze() -> None:
    a, spark, table = _ctx()
    target = table("bronze", "trips")
    with monitoring.recorded(spark, table("ops", "pipeline_runs"), a.run_id, "bronze", target) as run:
        raw = spark.table(a.source_table)
        n_out = _write(transforms.to_bronze(raw, a.source_table, a.run_id), target)
        run.set(rows_in=raw.count(), rows_out=n_out)
        print(f"bronze: wrote {n_out:,} rows to {target}")


def run_silver() -> None:
    a, spark, table = _ctx()
    target = table("silver", "trips")
    with monitoring.recorded(spark, table("ops", "pipeline_runs"), a.run_id, "silver", target) as run:
        bronze = spark.table(table("bronze", "trips"))
        clean, rejected = transforms.to_silver(bronze)
        total, n_rejected = bronze.count(), rejected.count()
        rate = transforms.reject_rate(total, n_rejected)
        run.set(rows_in=total, rows_rejected=n_rejected, reject_rate=rate)
        print(f"silver: {n_rejected:,} of {total:,} rows rejected ({rate:.2%})")
        _write(rejected, table("silver", "trips_rejected"))  # kept even when the gate fails
        # Quality gate: fail the task (and stop gold) before bad data is published.
        if rate > a.max_reject_rate:
            raise SystemExit(f"reject rate {rate:.2%} exceeds {a.max_reject_rate:.2%}")
        run.set(rows_out=_write(clean, target))


def run_gold() -> None:
    a, spark, table = _ctx()
    target = table("gold", "daily_zip_summary")
    with monitoring.recorded(spark, table("ops", "pipeline_runs"), a.run_id, "gold", target) as run:
        silver = spark.table(table("silver", "trips"))
        n_out = _write(transforms.to_gold(silver), target)
        run.set(rows_in=silver.count(), rows_out=n_out)
        print(f"gold: wrote {n_out:,} rows to {target}")


def run_governance() -> None:
    """Apply tags, then row filters on every column tagged rls=<dimension>."""
    a, spark, table = _ctx()
    schemas = {layer: getattr(a, f"{layer}_schema") for layer in governance.SCHEMA_TAGS}
    with monitoring.recorded(spark, table("ops", "pipeline_runs"), a.run_id, "governance", a.catalog) as run:
        owner = spark.sql("SELECT current_user()").first()[0]
        stmts = governance.tag_sql(a.catalog, schemas)
        stmts += governance.access_table_sql(a.catalog, schemas["ops"], owner)
        for s in stmts:
            spark.sql(s)
        tagged = [tuple(r) for r in spark.sql(
            governance.column_tags_query(a.catalog, list(schemas.values()))).collect()]
        filters = governance.row_filter_sql(a.catalog, schemas["ops"], tagged)
        filters.append(governance.monitoring_view_sql(a.catalog, schemas["ops"], schemas["gold"]))
        for s in filters:
            spark.sql(s)
        run.set(rows_out=len(tagged))
        print(f"governance: {len(stmts)} tag/access statements, row filters on {len(tagged)} column(s)")
