"""Builds and executes walkthrough.ipynb:  cd notebooks && ../.venv/bin/python _build.py"""
import nbformat as nbf
from nbconvert.preprocessors import ExecutePreprocessor

cells = [
    ("md", """# Medallion walkthrough: bronze → silver → gold

This notebook runs the project's real transform code (`src/medallion/transforms.py`) on a
**tiny made-up batch of taxi trips**, using Spark on this machine. On Databricks the same
functions run on the full `samples.nyctaxi.trips` table; only the reading and writing differ.

The three tiers:
- **Bronze**: keep everything exactly as it arrived, plus a note of where and when.
- **Silver**: keep only trustworthy rows; set the bad ones aside rather than deleting them.
- **Gold**: summarise for the people asking questions."""),
    ("code", """import os, sys; sys.path.insert(0, "../src")
from pyspark.sql import functions as F
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from medallion import common, governance, transforms

spark = (SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false")
         .config("spark.ui.showConsoleProgress", "false")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")"""),
    ("md", """## A small batch of raw trips

Six rows, deliberately messy: one exact duplicate, one trip that ends before it starts,
and one with a $0 fare. Real feeds look like this more often than you'd hope."""),
    ("code", """t = lambda h, m: datetime(2016, 2, 14, h, m, tzinfo=timezone.utc)
rows = [
    (t(8, 0),  t(8, 30), 3.0, 12.5, 10001, 10002),
    (t(8, 0),  t(8, 30), 3.0, 12.5, 10001, 10002),  # duplicate
    (t(9, 0),  t(9, 20), 5.0, 20.0, 10001, 10003),
    (t(9, 15), t(9, 25), 1.0,  7.5, 11201, 10001),  # Brooklyn
    (t(10, 0), t(9, 50), 2.0,  9.0, 10002, 10001),  # drop-off before pick-up
    (t(11, 0), t(11, 5), 0.5,  0.0, 10002, 10002),  # $0 fare
]
raw = spark.createDataFrame(rows, "tpep_pickup_datetime timestamp, tpep_dropoff_datetime timestamp, "
                            "trip_distance double, fare_amount double, pickup_zip int, dropoff_zip int")
raw.show()"""),
    ("md", """## Bronze: keep everything, add lineage

Nothing is removed. Two columns record *when* the row was ingested and *from where*,
so if a bug is found in silver later, it can be rebuilt from bronze."""),
    ("code", """bronze = transforms.to_bronze(raw, "samples.nyctaxi.trips")
print(bronze.count(), "rows in bronze")
bronze.select("tpep_pickup_datetime", "fare_amount", "_source").show(truncate=False)"""),
    ("md", """## Silver: validate, de-duplicate, derive

`to_silver` returns two tables: the clean trips and the rejects. The job compares the
reject share with a limit (5% by default). Here it's 2 of 6 rows (33%), so on Databricks
this batch would **fail the quality gate** and gold would not be updated."""),
    ("code", """clean, rejected = transforms.to_silver(bronze)
rate = transforms.reject_rate(bronze.count(), rejected.count())
print(f"clean: {clean.count()}  rejected: {rejected.count()}  reject rate: {rate:.0%}  "
      f"gate (5%): {'FAIL' if rate > 0.05 else 'pass'}")
clean.select("pickup_date", "pickup_zip", "pickup_borough", "fare_amount", "trip_minutes").show()"""),
    ("md", """Each rejected row says *which* rules it broke (`_reject_reasons`). The rules are a plain
dictionary of name → condition (`transforms.silver_rules()`), applied by the reusable
`common.split_valid`, so another pipeline gets the same behaviour with its own rules."""),
    ("code", """rejected.select("tpep_pickup_datetime", "tpep_dropoff_datetime", "fare_amount", "_reject_reasons").show(truncate=False)"""),
    ("md", """The duplicate disappeared without counting as a reject. It's not *bad* data, just
repeated data. The two impossible trips are kept in `silver.trips_rejected` for review.

## Gold: the business view

One row per day per pick-up ZIP: how many trips and how much money."""),
    ("code", """transforms.to_gold(clean).orderBy("pickup_zip").show()"""),
    ("md", """## Reusable building blocks

`common.py` holds expressions that know nothing about taxis: tidy column names, trim text,
validate with named rules, de-duplicate, bucket numbers into labels, summarise. A few of
them on their own:"""),
    ("code", """messy = spark.createDataFrame([("  Alice ", 3, 1), ("Bob", 15, 1), ("Bob", 15, 2)],
                              "`Customer Name` string, OrderSize int, version int")
tidy = common.trim_strings(common.snake_case_columns(messy))
tidy = common.dedupe_latest(tidy, ["customer_name"], order_by="version")
tidy.withColumn("size", common.bucket(F.col("order_size"), [(0, 9, "small"), (10, 99, "large")])).show()"""),
    ("md", """## Who sees which rows: tags drive row-level security

Gold's `pickup_borough` column carries the tag `rls = borough`. After each load, the job's
governance task:

1. applies the descriptive tags (layer, domain, sensitivity) to schemas, tables and columns;
2. looks up every column tagged `rls` in Unity Catalog;
3. attaches a row filter to it, which checks the `ops.rls_access` table:
   *is the current user (or a group they're in) allowed to see this borough?*

Securing another table is one tag, no new code. Unity Catalog doesn't run on a laptop,
so here is the SQL the task would run (with dev-mode schema names):"""),
    ("code", """schemas = {"bronze": "dev_me_bronze", "silver": "dev_me_silver", "gold": "dev_me_gold", "ops": "dev_me_ops"}
for stmt in governance.tag_sql("workspace", schemas)[-3:]:
    print(stmt, end="\\n\\n")
tagged = [("dev_me_gold", "daily_zip_summary", "pickup_borough", "borough")]
for stmt in governance.row_filter_sql("workspace", "dev_me_ops", tagged):
    print(stmt, end="\\n\\n")"""),
    ("md", """## How this gets to Databricks

Nothing above is specific to a laptop. The bundle (`databricks.yml` + `resources/`)
packages this code as a Python wheel, creates the `bronze`/`silver`/`gold`/`ops` schemas in
Unity Catalog, and defines a cron-scheduled job whose four tasks call these functions in
order. Every task also writes a row of run metrics to `ops.pipeline_runs`, which feeds the
*Medallion pipeline monitor* dashboard. GitHub Actions runs the same tests on every change
and deploys the bundle on merge."""),
    ("code", """spark.stop()"""),
]

nb = nbf.v4.new_notebook()
nb.cells = [nbf.v4.new_markdown_cell(s) if k == "md" else nbf.v4.new_code_cell(s) for k, s in cells]
nb.metadata["kernelspec"] = {"name": "python3", "display_name": "Python 3", "language": "python"}
ExecutePreprocessor(timeout=300, kernel_name="python3").preprocess(nb, {"metadata": {"path": "."}})
for cell in nb.cells:  # drop Spark's JVM start-up chatter
    if cell.cell_type == "code":
        cell.outputs = [o for o in cell.outputs if o.get("name") != "stderr"]
nbf.write(nb, "walkthrough.ipynb")
