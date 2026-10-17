"""The dashboard JSON is only checked by Databricks at deploy time; catch broken
references here first."""
import json
import re
from pathlib import Path

DASH = json.loads(Path(__file__).parents[1].joinpath(
    "src/dashboards/medallion_monitor.lv.dashboard.json").read_text())
WIDGETS = [item["widget"] for page in DASH["pages"] for item in page["layout"]]


def test_every_widget_uses_a_defined_dataset():
    names = {d["name"] for d in DASH["datasets"]}
    used = {q["query"]["datasetName"] for w in WIDGETS for q in w.get("queries", [])}
    assert used <= names


def test_encodings_reference_query_fields():
    for w in WIDGETS:
        if "spec" not in w:
            continue
        fields = {f["name"] for q in w["queries"] for f in q["query"]["fields"]}
        enc = w["spec"]["encodings"]
        refs = [c["fieldName"] for c in enc.get("columns", [])]
        refs += [v["fieldName"] for k, v in enc.items() if isinstance(v, dict)]
        assert set(refs) <= fields, w["name"]


def test_datasets_only_read_ops_schema_objects():
    """dataset_schema points queries at ops, so tables must be unqualified ops objects."""
    for d in DASH["datasets"]:
        tables = re.findall(r"FROM\s+([\w.`]+)", " ".join(d["queryLines"]))
        assert all("." not in t for t in tables), d["name"]


def test_widget_positions_fit_six_column_grid_without_overlap():
    for page in DASH["pages"]:
        cells = set()
        for item in page["layout"]:
            p = item["position"]
            assert p["x"] + p["width"] <= 6
            box = {(x, y) for x in range(p["x"], p["x"] + p["width"])
                   for y in range(p["y"], p["y"] + p["height"])}
            assert not cells & box, item["widget"]["name"]
            cells |= box
