from datetime import datetime, timezone

import pytest

from medallion import transforms

COLS = transforms.SOURCE_COLUMNS
T0 = datetime(2016, 2, 14, 8, 0, tzinfo=timezone.utc)
T1 = datetime(2016, 2, 14, 8, 30, tzinfo=timezone.utc)


def raw(spark, rows):
    return spark.createDataFrame(
        rows,
        "tpep_pickup_datetime timestamp, tpep_dropoff_datetime timestamp, "
        "trip_distance double, fare_amount double, pickup_zip int, dropoff_zip int",
    )


GOOD = (T0, T1, 3.0, 12.5, 10001, 10002)


def test_bronze_keeps_every_row_and_adds_lineage(spark):
    bad = (T1, T0, -1.0, 0.0, None, None)
    out = transforms.to_bronze(raw(spark, [GOOD, bad]), "samples.nyctaxi.trips")
    assert out.count() == 2
    assert set(out.columns) == set(COLS) | {"_ingested_at", "_source"}
    assert {r._source for r in out.collect()} == {"samples.nyctaxi.trips"}


@pytest.mark.parametrize(
    "bad",
    [
        (T1, T0, 3.0, 12.5, 10001, 10002),    # dropoff before pickup
        (T0, T1, 0.0, 12.5, 10001, 10002),    # zero distance
        (T0, T1, 3.0, -5.0, 10001, 10002),    # negative fare
        (T0, T1, 3.0, 12.5, None, 10002),     # no pickup zip
        (None, T1, 3.0, 12.5, 10001, 10002),  # null timestamp (must reject, not vanish)
    ],
)
def test_silver_rejects_each_bad_row_type(spark, bad):
    bronze = transforms.to_bronze(raw(spark, [GOOD, bad]), "test")
    clean, rejected = transforms.to_silver(bronze)
    assert clean.count() == 1
    assert rejected.count() == 1


def test_silver_dedupes_and_derives_fields(spark):
    bronze = transforms.to_bronze(raw(spark, [GOOD, GOOD]), "test")
    clean, rejected = transforms.to_silver(bronze)
    rows = clean.collect()
    assert len(rows) == 1 and rejected.count() == 0
    assert rows[0].trip_minutes == 30.0
    assert str(rows[0].pickup_date) == "2016-02-14"
    assert "_ingested_at" not in clean.columns


def test_gold_aggregates_per_day_and_zip(spark):
    other_zip = (T0, T1, 1.0, 7.5, 10003, 10002)
    second = (T0.replace(hour=9), T1.replace(hour=9), 5.0, 20.0, 10001, 10002)
    clean, _ = transforms.to_silver(
        transforms.to_bronze(raw(spark, [GOOD, second, other_zip]), "test")
    )
    gold = {r.pickup_zip: r for r in transforms.to_gold(clean).collect()}
    assert gold[10001].trips == 2
    assert gold[10001].revenue == 32.5
    assert gold[10001].avg_distance == 4.0
    assert gold[10003].trips == 1


def test_reject_rate_handles_empty_input():
    assert transforms.reject_rate(0, 0) == 0.0
    assert transforms.reject_rate(200, 5) == 0.025
