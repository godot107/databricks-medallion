"""Run metrics: every task appends one row to ops.pipeline_runs.

The dashboard reads that table, so "is the pipeline healthy?" is a chart, not
a hunt through job logs: rows in/out per layer, reject rate, duration, status.
"""
import time
from contextlib import contextmanager
from datetime import datetime, timezone

from pyspark.sql import SparkSession

SCHEMA = (
    "run_id STRING, task STRING, target_table STRING, rows_in BIGINT, rows_out BIGINT, "
    "rows_rejected BIGINT, reject_rate DOUBLE, status STRING, message STRING, "
    "started_at TIMESTAMP, finished_at TIMESTAMP, duration_s DOUBLE"
)


class Run:
    """Mutable holder the task fills in; written once when the task ends."""

    def __init__(self, run_id: str, task: str, target_table: str):
        self.row = {
            "run_id": run_id, "task": task, "target_table": target_table,
            "rows_in": None, "rows_out": None, "rows_rejected": None, "reject_rate": None,
            "status": "running", "message": None,
            "started_at": datetime.now(timezone.utc), "finished_at": None, "duration_s": None,
        }
        self._t0 = time.monotonic()

    def set(self, **values) -> None:
        self.row.update(values)

    def finish(self, status: str, message: str | None = None) -> dict:
        self.row.update(
            status=status, message=message,
            finished_at=datetime.now(timezone.utc),
            duration_s=round(time.monotonic() - self._t0, 2),
        )
        return self.row


@contextmanager
def recorded(spark: SparkSession, metrics_table: str, run_id: str, task: str, target_table: str):
    """Record success or failure of the wrapped block, then re-raise any failure."""
    run = Run(run_id, task, target_table)
    try:
        yield run
    except BaseException as e:  # SystemExit from the quality gate counts too
        _append(spark, metrics_table, run.finish("failed", str(e)[:500]))
        raise
    _append(spark, metrics_table, run.finish("succeeded"))


def _append(spark: SparkSession, table: str, row: dict) -> None:
    spark.createDataFrame([row], SCHEMA).write.mode("append").saveAsTable(table)
