"""Unity Catalog tags and tag-driven row-level security, as code.

Tags are labels on schemas, tables and columns. Two kinds are used here:
- descriptive tags (layer, domain, sensitivity) for discovery and audit;
- the `rls` column tag, which says "filter rows of this table by this column".

`row_filter_sql` turns every column tagged rls=<dimension> into
`ALTER TABLE ... SET ROW FILTER rls_<dimension> ON (col)`, so securing a new
table is one tag, not new code. Who sees what lives in the `rls_access` table:
(principal, dimension, value), where principal is a user email or a group and
value '*' means every row.

On a paid account with governed tags, the same intent can be one ABAC policy
(sql/abac_policy.sql); Free Edition has no account console for governed tags.

Each function returns SQL strings so the logic is unit-tested without a
workspace; jobs.run_governance executes them.
"""
from collections.abc import Mapping

# Layer name -> tags on that schema. Keys are layer names; real schema names
# (dev mode prefixes them) are passed in at run time.
SCHEMA_TAGS: Mapping[str, Mapping[str, str]] = {
    "bronze": {"layer": "bronze", "domain": "mobility", "sensitivity": "internal"},
    "silver": {"layer": "silver", "domain": "mobility", "sensitivity": "internal"},
    "gold": {"layer": "gold", "domain": "mobility", "sensitivity": "restricted"},
    "ops": {"layer": "ops", "domain": "platform", "sensitivity": "internal"},
}

# (layer, table) -> table tags
TABLE_TAGS: Mapping[tuple[str, str], Mapping[str, str]] = {
    ("bronze", "trips"): {"layer": "bronze", "contains_pii": "false", "refresh": "daily"},
    ("silver", "trips"): {"layer": "silver", "contains_pii": "false", "refresh": "daily"},
    ("silver", "trips_rejected"): {"layer": "silver", "quality": "rejects"},
    ("gold", "daily_zip_summary"): {"layer": "gold", "certified": "true", "refresh": "daily"},
    ("ops", "pipeline_runs"): {"layer": "ops", "purpose": "monitoring"},
}

# (layer, table, column) -> column tags. rls=<dimension> turns on row filtering.
COLUMN_TAGS: Mapping[tuple[str, str, str], Mapping[str, str]] = {
    ("gold", "daily_zip_summary", "pickup_borough"): {"rls": "borough", "geo": "borough"},
    ("gold", "daily_zip_summary", "pickup_zip"): {"geo": "zip"},
    ("gold", "daily_zip_summary", "revenue"): {"metric": "currency_usd"},
}


def rls_dimensions() -> list[str]:
    return sorted({t["rls"] for t in COLUMN_TAGS.values() if "rls" in t})


def _q(*parts: str) -> str:
    return ".".join(f"`{p}`" for p in parts)


def _tag_list(tags: Mapping[str, str]) -> str:
    return ", ".join(f"'{k}' = '{v}'" for k, v in tags.items())


def tag_sql(catalog: str, schemas: Mapping[str, str]) -> list[str]:
    """SET TAGS statements; `schemas` maps layer -> deployed schema name."""
    stmts = [
        f"ALTER SCHEMA {_q(catalog, schemas[layer])} SET TAGS ({_tag_list(t)})"
        for layer, t in SCHEMA_TAGS.items()
    ]
    stmts += [
        f"ALTER TABLE {_q(catalog, schemas[layer], table)} SET TAGS ({_tag_list(t)})"
        for (layer, table), t in TABLE_TAGS.items()
    ]
    stmts += [
        f"ALTER TABLE {_q(catalog, schemas[layer], table)} "
        f"ALTER COLUMN `{col}` SET TAGS ({_tag_list(t)})"
        for (layer, table, col), t in COLUMN_TAGS.items()
    ]
    return stmts


def access_table_sql(catalog: str, ops_schema: str, owner: str) -> list[str]:
    """Create the access map and make sure the pipeline owner sees every row.

    Without the owner's '*' row, the people who run and debug the pipeline
    would be filtered out of their own gold table.
    """
    table = _q(catalog, ops_schema, "rls_access")
    dims = ", ".join(f"('{d}')" for d in rls_dimensions())
    return [
        f"CREATE TABLE IF NOT EXISTS {table} "
        "(principal STRING NOT NULL, dimension STRING NOT NULL, value STRING NOT NULL) "
        "COMMENT 'Row-level security map: principal (user or group) may see rows where "
        "<dimension> = value; value * = all rows'",
        f"MERGE INTO {table} t USING (SELECT '{owner}' AS principal, d AS dimension, '*' AS value "
        f"FROM VALUES {dims} AS v(d)) s "
        "ON t.principal = s.principal AND t.dimension = s.dimension AND t.value = s.value "
        "WHEN NOT MATCHED THEN INSERT *",
    ]


def filter_function_sql(catalog: str, ops_schema: str, dimension: str) -> str:
    """The row filter: TRUE when the current user (or a group they're in) may see the value."""
    access = _q(catalog, ops_schema, "rls_access")
    fn = _q(catalog, ops_schema, f"rls_{dimension}")
    return (
        f"CREATE OR REPLACE FUNCTION {fn}(v STRING) RETURNS BOOLEAN "
        f"COMMENT 'Row filter for columns tagged rls={dimension}; rules in rls_access' "
        f"RETURN EXISTS (SELECT 1 FROM {access} a "
        f"WHERE a.dimension = '{dimension}' AND a.value IN ('*', v) "
        "AND (a.principal = current_user() OR is_account_group_member(a.principal)))"
    )


def column_tags_query(catalog: str, schema_names: list[str]) -> str:
    """Find every column tagged rls=<dimension> in our schemas (read from UC, not from code)."""
    in_list = ", ".join(f"'{s}'" for s in schema_names)
    return (
        "SELECT schema_name, table_name, column_name, tag_value AS dimension "
        f"FROM {_q(catalog, 'information_schema', 'column_tags')} "
        f"WHERE tag_name = 'rls' AND schema_name IN ({in_list})"
    )


def row_filter_sql(catalog: str, ops_schema: str, tagged: list[tuple[str, str, str, str]]) -> list[str]:
    """One function per dimension, then SET ROW FILTER on every tagged column."""
    dims = sorted({d for *_, d in tagged})
    stmts = [filter_function_sql(catalog, ops_schema, d) for d in dims]
    stmts += [
        f"ALTER TABLE {_q(catalog, schema, table)} SET ROW FILTER "
        f"{_q(catalog, ops_schema, f'rls_{dim}')} ON (`{col}`)"
        for schema, table, col, dim in tagged
    ]
    return stmts


def monitoring_view_sql(catalog: str, ops_schema: str, gold_schema: str) -> str:
    """Dashboard view over gold. Row filters still apply to whoever queries it."""
    return (
        f"CREATE OR REPLACE VIEW {_q(catalog, ops_schema, 'v_gold_daily_borough')} AS "
        "SELECT pickup_date, pickup_borough, SUM(trips) AS trips, ROUND(SUM(revenue), 2) AS revenue "
        f"FROM {_q(catalog, gold_schema, 'daily_zip_summary')} GROUP BY pickup_date, pickup_borough"
    )
