# TODO

## Prepared Statements: `SELECT *` column metadata

`handle_stmt_prepare()` derives result columns from AST projections only.
`SELECT *` (and qualified wildcards) require catalog lookup to resolve
column names/types, so `select_column_names()` returns `None` and we
report `num_columns=0` in COM_STMT_PREPARE_OK.

mysql_async tolerates this; C libmysqlclient may hang or misbehave.
Fix requires running the resolver against the catalog at prepare time
to expand wildcards and produce accurate column definitions.
