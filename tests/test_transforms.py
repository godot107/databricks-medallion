from datetime import datetime, timezone

import pytest

from medallion import transforms

T0 = datetime(2016, 2, 14, 8, 0, tzinfo=timezone.utc)
T1 = datetime(2016, 2, 14, 8, 30, tzinfo=timezone.utc)
GOOD = (T0, T1, 3.0, 12.5, 10001, 10002)


def raw(spark, rows):
    return spark.createDataFrame(
        rows,
        "tpep_pickup_datetime timestamp, tpep_dropoff_datetime timestamp, "
        "trip_distance double, fare_amount double, pickup_zip int, dropoff_zip int",
    )


def test_bronze_keeps_every_row_and_adds_lineage(spark):
    bad = (T1, T0, -1.0, 0.0, None, None)
    out = transforms.to_bronze(raw(spark, [GOOD, bad]), "samples.nyctaxi.trips", "42")
    assert out.count() == 2
    assert set(out.columns) == set(transforms.SOURCE_COLUMNS) | {"_ingested_at", "_source", "_run_id"}
    assert {(r._source, r._run_id) for r in out.collect()} == {("samples.nyctaxi.trips", "42")}


@pytest.mark.parametrize(
    "bad, reason",
    [
        ((T1, T0, 3.0, 12.5, 10001, 10002), "dropoff_before_pickup"),
        ((T0, T1, 0.0, 12.5, 10001, 10002), "non_positive_distance"),
        ((T0, T1, 3.0, -5.0, 10001, 10002), "non_positive_fare"),
        ((T0, T1, 3.0, 12.5, None, 10002), "missing_pickup_zip"),
        ((None, T1, 3.0, 12.5, 10001, 10002), "missing_pickup_time"),
    ],
)
def test_silver_rejects_each_bad_row_type_with_reason(spark, bad, reason):
    clean, rejected = transforms.to_silver(transforms.to_bronze(raw(spark, [GOOD, bad]), "t"))
    assert clean.count() == 1
    [row] = rejected.collect()
    assert reason in row._reject_reasons


def test_silver_dedupes_and_derives_fields(spark):
    clean, rejected = transforms.to_silver(transforms.to_bronze(raw(spark, [GOOD, GOOD]), "t"))
    [row] = clean.collect()
    assert rejected.count() == 0
    assert row.trip_minutes == 30.0
    assert str(row.pickup_date) == "2016-02-14"
    assert row.pickup_borough == "Manhattan"
    assert not [c for c in clean.columns if c.startswith("_")]


def test_gold_aggregates_per_day_borough_and_zip(spark):
    queens = (T0, T1, 1.0, 7.5, 11101, 10002)
    second = (T0.replace(hour=9), T1.replace(hour=9), 5.0, 20.0, 10001, 10002)
    clean, _ = transforms.to_silver(transforms.to_bronze(raw(spark, [GOOD, second, queens]), "t"))
    gold = {r.pickup_zip: r for r in transforms.to_gold(clean).collect()}
    assert gold[10001].trips == 2 and gold[10001].pickup_borough == "Manhattan"
    assert gold[10001].revenue == 32.5
    assert gold[10001].avg_trip_distance == 4.0
    assert gold[11101].pickup_borough == "Queens"
