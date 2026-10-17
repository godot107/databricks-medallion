"""Taxi-specific layers, composed from the reusable helpers in common.py.

No reads or writes here, so every function runs the same on a laptop
(pytest + local Spark) and on Databricks serverless. jobs.py does the I/O.
"""
from pyspark.sql import DataFrame, functions as F

from medallion import common

SOURCE_COLUMNS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "fare_amount",
    "pickup_zip",
    "dropoff_zip",
]

SOURCE_TYPES = {
    "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp",
    "trip_distance": "double",
    "fare_amount": "double",
    "pickup_zip": "int",
    "dropoff_zip": "int",
}

# NYC ZIP ranges by borough (USPS prefixes). Coarse but enough to drive
# row-level security: an analyst can be limited to their borough's rows.
BOROUGH_ZIPS = [
    (10001, 10299, "Manhattan"),
    (10301, 10314, "Staten Island"),
    (10451, 10475, "Bronx"),
    (11004, 11109, "Queens"),
    (11351, 11697, "Queens"),
    (11201, 11256, "Brooklyn"),
]


def silver_rules() -> dict:
    """Silver's quality rules: name -> condition a good row must satisfy.

    A function, not a constant: Column objects need an active Spark session.
    """
    return {
        "missing_pickup_time": F.col("tpep_pickup_datetime").isNotNull(),
        "missing_dropoff_time": F.col("tpep_dropoff_datetime").isNotNull(),
        "dropoff_before_pickup": F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"),
        "non_positive_distance": F.col("trip_distance") > 0,
        "non_positive_fare": F.col("fare_amount") > 0,
        "missing_pickup_zip": F.col("pickup_zip").isNotNull(),
    }


def to_bronze(raw: DataFrame, source: str, run_id: str | None = None) -> DataFrame:
    """Bronze = the raw rows as they arrived, plus where and when we got them.

    Nothing is cleaned or dropped: bronze is the replayable record.
    """
    df = common.snake_case_columns(raw).select(*SOURCE_COLUMNS)
    return common.add_audit_columns(df, source, run_id)


def to_silver(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Silver = trusted rows: typed, valid, de-duplicated, with derived fields.

    Returns (clean, rejected); rejected rows carry `_reject_reasons` so the job
    can report and gate on the reject rate instead of silently dropping data.
    """
    typed = common.cast_columns(common.trim_strings(bronze), SOURCE_TYPES)
    valid, rejected = common.split_valid(typed, silver_rules())
    clean = common.dedupe_exact(common.drop_audit_columns(valid), SOURCE_COLUMNS).withColumns(
        {
            "pickup_date": F.to_date("tpep_pickup_datetime"),
            "pickup_borough": common.bucket(F.col("pickup_zip"), BOROUGH_ZIPS, "Other"),
            "trip_minutes": common.minutes_between("tpep_pickup_datetime", "tpep_dropoff_datetime"),
        }
    )
    return clean, rejected


def to_gold(silver: DataFrame) -> DataFrame:
    """Gold = daily trips and revenue per pick-up borough and ZIP.

    `pickup_borough` is the row-level-security column (tagged rls=borough).
    """
    return common.summarize(
        silver,
        by=["pickup_date", "pickup_borough", "pickup_zip"],
        sums=["fare_amount"],
        avgs=["fare_amount", "trip_distance", "trip_minutes"],
        count_as="trips",
    ).withColumnRenamed("total_fare_amount", "revenue")


reject_rate = common.reject_rate
