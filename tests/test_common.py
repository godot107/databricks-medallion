from pyspark.sql import functions as F

from medallion import common


def test_snake_case_variants():
    assert common.snake_case("PickUp Zip") == "pick_up_zip"
    assert common.snake_case("pickupZip") == "pickup_zip"
    assert common.snake_case(" fare-amount ($) ") == "fare_amount"


def test_trim_strings_blanks_become_null(spark):
    df = spark.createDataFrame([("  a ", 1), ("   ", 2)], "s string, n int")
    assert [r.s for r in common.trim_strings(df).orderBy("n").collect()] == ["a", None]


def test_split_valid_lists_every_failed_rule_and_treats_null_as_failure(spark):
    df = spark.createDataFrame([(1, 5), (-1, None), (2, -3)], "a int, b int")
    rules = {"a_positive": F.col("a") > 0, "b_positive": F.col("b") > 0}
    valid, rejected = common.split_valid(df, rules)
    assert [r.a for r in valid.collect()] == [1]
    reasons = {r.a: r._reject_reasons for r in rejected.collect()}
    assert reasons == {-1: ["a_positive", "b_positive"], 2: ["b_positive"]}


def test_dedupe_latest_keeps_newest_per_key(spark):
    df = spark.createDataFrame([("k", 1, "old"), ("k", 2, "new"), ("j", 1, "only")], "key string, v int, label string")
    out = {r.key: r.label for r in common.dedupe_latest(df, ["key"], "v").collect()}
    assert out == {"k": "new", "j": "only"}


def test_bucket_ranges_and_default(spark):
    df = spark.createDataFrame([(5,), (15,), (99,)], "x int")
    out = df.select(common.bucket(F.col("x"), [(0, 9, "low"), (10, 19, "mid")], "other").alias("b"))
    assert [r.b for r in out.collect()] == ["low", "mid", "other"]


def test_summarize_names_columns_predictably(spark):
    df = spark.createDataFrame([("a", 1.0), ("a", 3.0), ("b", 2.0)], "g string, v double")
    out = {r.g: r for r in common.summarize(df, ["g"], sums=["v"], avgs=["v"]).collect()}
    assert (out["a"].rows, out["a"].total_v, out["a"].avg_v) == (2, 4.0, 2.0)


def test_reject_rate_handles_empty_input():
    assert common.reject_rate(0, 0) == 0.0
    assert common.reject_rate(200, 5) == 0.025
