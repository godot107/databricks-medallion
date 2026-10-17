-- Try row-level security by hand (SQL editor). Replace `workspace`/schema names
-- with yours: dev mode names them dev_<you>_gold / dev_<you>_ops.

-- 1. What tags are on gold?
SELECT table_name, column_name, tag_name, tag_value
FROM workspace.information_schema.column_tags
WHERE schema_name LIKE '%gold';

-- 2. Who can see what?
SELECT * FROM workspace.ops.rls_access;

-- 3. Restrict yourself to Manhattan and watch gold shrink.
UPDATE workspace.ops.rls_access SET value = 'Manhattan'
WHERE principal = current_user() AND dimension = 'borough';
SELECT pickup_borough, SUM(trips) FROM workspace.gold.daily_zip_summary GROUP BY 1;

-- 4. Put yourself back (the governance task also re-adds your '*' row on its next run).
UPDATE workspace.ops.rls_access SET value = '*'
WHERE principal = current_user() AND dimension = 'borough';

-- 5. Grant a group (on a paid account, groups come from the account console / SCIM).
INSERT INTO workspace.ops.rls_access VALUES ('brooklyn-analysts', 'borough', 'Brooklyn');
