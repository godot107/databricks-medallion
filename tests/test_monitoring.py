import pytest

from medallion import monitoring


class FakeSpark:
    """Captures appended rows instead of writing tables."""
    def __init__(self):
        self.rows = []


@pytest.fixture
def captured(monkeypatch):
    fake = FakeSpark()
    monkeypatch.setattr(monitoring, "_append", lambda spark, table, row: fake.rows.append(row))
    return fake


def test_success_is_recorded_with_metrics(captured):
    with monitoring.recorded(None, "ops.pipeline_runs", "7", "gold", "gold.t") as run:
        run.set(rows_in=10, rows_out=3)
    [row] = captured.rows
    assert (row["status"], row["rows_out"], row["run_id"]) == ("succeeded", 3, "7")
    assert row["duration_s"] >= 0 and row["finished_at"] >= row["started_at"]


def test_failure_is_recorded_then_reraised(captured):
    with pytest.raises(SystemExit):
        with monitoring.recorded(None, "ops.pipeline_runs", "7", "silver", "silver.t") as run:
            run.set(reject_rate=0.2)
            raise SystemExit("reject rate 20.00% exceeds 5.00%")
    [row] = captured.rows
    assert row["status"] == "failed" and "exceeds" in row["message"]
    assert row["reject_rate"] == 0.2


def test_metrics_row_matches_table_schema(spark):
    row = monitoring.Run("1", "bronze", "t").finish("succeeded")
    df = spark.createDataFrame([row], monitoring.SCHEMA)
    assert df.count() == 1 and df.columns[0] == "run_id"
