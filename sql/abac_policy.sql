-- Paid accounts only (NOT Free Edition): the same borough security as one
-- attribute-based (ABAC) policy instead of per-table row filters.
--
-- Needs: a governed tag `rls` with allowed value `borough` (created by an account
-- admin under Catalog -> Governed tags), serverless or DBR 16.4+, and MANAGE on the
-- schema. ABAC is Public Preview on AWS as of 2026-10.
-- Docs: https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/policies
--
-- With this policy, any table in the schema whose column carries rls=borough is
-- filtered automatically, with no governance task needed. Replace prod/gold with your names.

CREATE OR REPLACE FUNCTION prod.ops.rls_borough(v STRING) RETURNS BOOLEAN
RETURN EXISTS (
  SELECT 1 FROM prod.ops.rls_access a
  WHERE a.dimension = 'borough' AND a.value IN ('*', v)
    AND (a.principal = current_user() OR is_account_group_member(a.principal))
);

CREATE OR REPLACE POLICY borough_row_security
ON SCHEMA prod.gold
COMMENT 'Analysts see only the boroughs granted to them in ops.rls_access'
ROW FILTER prod.ops.rls_borough
TO `account users`
FOR TABLES
MATCH COLUMNS has_tag_value('rls', 'borough') AS borough_col
USING COLUMNS (borough_col);
