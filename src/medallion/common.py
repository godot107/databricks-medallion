"""Reusable transformation expressions for any medallion pipeline.

Nothing here knows about taxis: each helper takes a DataFrame (or Column) and
returns one, so layers are short compositions of these. transforms.py shows the
taxi-specific use.
"""
import re
from collections.abc import Mapping, Sequence

from pyspark.sql import Column, DataFrame, Window, functions as F

REJECT_REASONS = "_reject_reasons"


# --- column hygiene -----------------------------------------------------------

def snake_case(name: str) -> str:
    """'PickUp Zip' / 'pickupZip' / 'pickup-zip' -> 'pickup_zip'."""
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name.strip())
    return re.sub(r"[^0-9a-zA-Z]+", "_", name).strip("_").lower()


def snake_case_columns(df: DataFrame) -> DataFrame:
    return df.toDF(*[snake_case(c) for c in df.columns])


def trim_strings(df: DataFrame) -> DataFrame:
    """Trim every string column and turn empty strings into NULL."""
    exprs = {
        f.name: F.nullif(F.trim(F.col(f.name)), F.lit(""))
        for f in df.schema.fields
        if f.dataType.typeName() == "string"
    }
    return df.withColumns(exprs) if exprs else df


def cast_columns(df: DataFrame, types: Mapping[str, str]) -> DataFrame:
    """Cast named columns, e.g. {"fare_amount": "double", "pickup_zip": "int"}."""
    return df.withColumns({c: F.col(c).cast(t) for c, t in types.items()})


# --- lineage ------------------------------------------------------------------

def add_audit_columns(df: DataFrame, source: str, run_id: str | None = None) -> DataFrame:
    """Stamp where and when each row was ingested (bronze lineage)."""
    return df.withColumns(
        {
            "_ingested_at": F.current_timestamp(),
            "_source": F.lit(source),
            "_run_id": F.lit(run_id).cast("string"),
        }
    )


def drop_audit_columns(df: DataFrame) -> DataFrame:
    return df.drop(*[c for c in df.columns if c.startswith("_")])


# --- quality ------------------------------------------------------------------

def split_valid(df: DataFrame, rules: Mapping[str, Column]) -> tuple[DataFrame, DataFrame]:
    """Apply named boolean rules; return (valid, rejected).

    Rejected rows keep every column plus `_reject_reasons`, the list of rule
    names they failed. A rule that evaluates to NULL counts as failed, so rows
    are never silently lost to three-valued logic.
    """
    failed = [F.when(~F.coalesce(rule, F.lit(False)), F.lit(name)) for name, rule in rules.items()]
    flagged = df.withColumn(REJECT_REASONS, F.filter(F.array(*failed), lambda x: x.isNotNull()))
    valid = flagged.filter(F.size(REJECT_REASONS) == 0).drop(REJECT_REASONS)
    rejected = flagged.filter(F.size(REJECT_REASONS) > 0)
    return valid, rejected


def dedupe_exact(df: DataFrame, cols: Sequence[str]) -> DataFrame:
    return df.dropDuplicates(list(cols))


def dedupe_latest(df: DataFrame, keys: Sequence[str], order_by: str) -> DataFrame:
    """Keep one row per key: the one with the greatest `order_by` (latest wins)."""
    w = Window.partitionBy(*keys).orderBy(F.col(order_by).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


# --- derived values -----------------------------------------------------------

def minutes_between(start: str, end: str, scale: int = 2) -> Column:
    return F.round((F.unix_timestamp(end) - F.unix_timestamp(start)) / 60, scale)


def bucket(col: Column, ranges: Sequence[tuple[int, int, str]], default: str | None = None) -> Column:
    """Map a numeric column to a label by inclusive ranges [(lo, hi, label), ...]."""
    expr = None
    for lo, hi, label in ranges:
        cond = col.between(lo, hi)
        expr = F.when(cond, label) if expr is None else expr.when(cond, label)
    return expr.otherwise(F.lit(default))


def summarize(df: DataFrame, by: Sequence[str], sums: Sequence[str] = (),
              avgs: Sequence[str] = (), count_as: str = "rows") -> DataFrame:
    """groupBy + count + rounded sums/averages with predictable names."""
    aggs = [F.count("*").alias(count_as)]
    aggs += [F.round(F.sum(c), 2).alias(f"total_{c}") for c in sums]
    aggs += [F.round(F.avg(c), 2).alias(f"avg_{c}") for c in avgs]
    return df.groupBy(*by).agg(*aggs)


def reject_rate(total: int, rejected: int) -> float:
    return rejected / total if total else 0.0
