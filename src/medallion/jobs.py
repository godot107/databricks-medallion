"""Job entry points (wheel console scripts) called by the bundle's job tasks.

Each layer reads the previous layer's table and overwrites its own, so a rerun
of any task gives the same result (idempotent).
"""
import argparse

from pyspark.sql import SparkSession

from medallion import transforms


def _args(*extra: str) -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--schema", required=True)
    for name in extra:
        p.add_argument(f"--{name}", required=True)
    return p.parse_args()


def _spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()


def _write(df, table: str) -> None:
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)


def run_bronze() -> None:
    a = _args("source_table")
    spark = _spark()
    bronze = transforms.to_bronze(spark.table(a.source_table), a.source_table)
    target = f"{a.catalog}.{a.schema}.trips"
    _write(bronze, target)
    print(f"bronze: wrote {spark.table(target).count():,} rows to {target}")


def run_silver() -> None:
    a = _args("bronze_schema", "max_reject_rate")
    spark = _spark()
    bronze = spark.table(f"{a.catalog}.{a.bronze_schema}.trips")
    clean, rejected = transforms.to_silver(bronze)
    total, n_rejected = bronze.count(), rejected.count()
    rate = transforms.reject_rate(total, n_rejected)
    print(f"silver: {n_rejected:,} of {total:,} rows rejected ({rate:.2%})")
    # Quality gate: fail the task (and stop gold) before bad data is published.
    if rate > float(a.max_reject_rate):
        raise SystemExit(f"reject rate {rate:.2%} exceeds {float(a.max_reject_rate):.2%}")
    _write(clean, f"{a.catalog}.{a.schema}.trips")
    _write(rejected, f"{a.catalog}.{a.schema}.trips_rejected")


def run_gold() -> None:
    a = _args("silver_schema")
    spark = _spark()
    silver = spark.table(f"{a.catalog}.{a.silver_schema}.trips")
    target = f"{a.catalog}.{a.schema}.daily_zip_summary"
    _write(transforms.to_gold(silver), target)
    print(f"gold: wrote {spark.table(target).count():,} rows to {target}")
