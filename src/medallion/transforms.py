"""Pure DataFrame -> DataFrame transforms, one per layer.

No reads or writes here, so every function runs the same on a laptop
(pytest + local Spark) and on Databricks serverless. jobs.py does the I/O.
"""
from pyspark.sql import DataFrame, functions as F

SOURCE_COLUMNS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "fare_amount",
    "pickup_zip",
    "dropoff_zip",
]


def to_bronze(raw: DataFrame, source: str) -> DataFrame:
    """Bronze = the raw rows as they arrived, plus where and when we got them.

    Nothing is cleaned or dropped: bronze is the replayable record.
    """
    return raw.select(*SOURCE_COLUMNS).withColumns(
        {"_ingested_at": F.current_timestamp(), "_source": F.lit(source)}
    )


def _is_valid() -> F.Column:
    return (
        F.col("tpep_pickup_datetime").isNotNull()
        & F.col("tpep_dropoff_datetime").isNotNull()
        & (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
        & (F.col("trip_distance") > 0)
        & (F.col("fare_amount") > 0)
        & F.col("pickup_zip").isNotNull()
    )


def to_silver(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Silver = trusted rows: valid, de-duplicated, with derived fields.

    Returns (clean, rejected) so the job can report and gate on the reject rate
    instead of silently dropping data.
    """
    flagged = bronze.withColumn("_valid", F.coalesce(_is_valid(), F.lit(False)))
    rejected = flagged.filter(~F.col("_valid")).drop("_valid")
    clean = (
        flagged.filter(F.col("_valid"))
        .drop("_valid", "_ingested_at", "_source")
        .dropDuplicates(SOURCE_COLUMNS)
        .withColumns(
            {
                "pickup_date": F.to_date("tpep_pickup_datetime"),
                "trip_minutes": F.round(
                    (
                        F.unix_timestamp("tpep_dropoff_datetime")
                        - F.unix_timestamp("tpep_pickup_datetime")
                    )
                    / 60,
                    2,
                ),
            }
        )
    )
    return clean, rejected


def to_gold(silver: DataFrame) -> DataFrame:
    """Gold = a business-ready table: daily trips and revenue per pickup ZIP."""
    return silver.groupBy("pickup_date", "pickup_zip").agg(
        F.count("*").alias("trips"),
        F.round(F.sum("fare_amount"), 2).alias("revenue"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
        F.round(F.avg("trip_minutes"), 2).alias("avg_minutes"),
    )


def reject_rate(total: int, rejected: int) -> float:
    return rejected / total if total else 0.0
