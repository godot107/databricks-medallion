from medallion import governance

SCHEMAS = {"bronze": "dev_me_bronze", "silver": "dev_me_silver", "gold": "dev_me_gold", "ops": "dev_me_ops"}


def test_tag_sql_uses_deployed_schema_names():
    stmts = governance.tag_sql("workspace", SCHEMAS)
    assert "ALTER SCHEMA `workspace`.`dev_me_gold` SET TAGS ('layer' = 'gold', 'domain' = 'mobility', 'sensitivity' = 'restricted')" in stmts
    assert any("ALTER COLUMN `pickup_borough` SET TAGS ('rls' = 'borough'" in s for s in stmts)
    assert not any("`gold`." in s for s in stmts)  # never the undeployed layer name


def test_every_tagged_object_has_a_known_layer():
    layers = set(governance.SCHEMA_TAGS)
    assert {k[0] for k in governance.TABLE_TAGS} <= layers
    assert {k[0] for k in governance.COLUMN_TAGS} <= layers


def test_access_map_grants_owner_every_dimension():
    create, merge = governance.access_table_sql("workspace", "dev_me_ops", "me@example.com")
    assert "CREATE TABLE IF NOT EXISTS `workspace`.`dev_me_ops`.`rls_access`" in create
    assert "'me@example.com' AS principal" in merge and "('borough')" in merge
    assert "WHEN NOT MATCHED THEN INSERT" in merge  # idempotent: rerun adds nothing


def test_row_filters_follow_tags_found_in_catalog():
    tagged = [("dev_me_gold", "daily_zip_summary", "pickup_borough", "borough"),
              ("dev_me_gold", "other_table", "region", "region")]
    stmts = governance.row_filter_sql("workspace", "dev_me_ops", tagged)
    fns = [s for s in stmts if s.startswith("CREATE OR REPLACE FUNCTION")]
    assert len(fns) == 2  # one filter function per dimension
    assert ("ALTER TABLE `workspace`.`dev_me_gold`.`daily_zip_summary` SET ROW FILTER "
            "`workspace`.`dev_me_ops`.`rls_borough` ON (`pickup_borough`)") in stmts
    assert "current_user()" in fns[0] and "is_account_group_member" in fns[0]


def test_column_tags_query_scopes_to_our_schemas():
    q = governance.column_tags_query("workspace", ["dev_me_gold", "dev_me_ops"])
    assert "information_schema`.`column_tags`" in q
    assert "tag_name = 'rls'" in q and "'dev_me_gold', 'dev_me_ops'" in q
