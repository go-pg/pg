# Changelog

## v8

- Added `QueryContext`, `ExecContext`, and `ModelContext` which accept `context.Context`. Queries are cancelled when context is cancelled.
- Model hooks are changed to accept `context.Context` as first argument.
- Fixed array and hstore parsers to handle multiple single quotes (#1235).

## v7

- DB.OnQueryProcessed is replaced with DB.AddQueryHook.
- Added WhereStruct.
- orm.Pager is moved to urlvalues.Pager. Pager.FromURLValues returns an error if page or limit params can't be parsed.

## v6.16

- Read buffer is re-worked. Default read buffer is increased to 65kb.

## v6.15

- Added Options.MinIdleConns.
- Options.MaxAge renamed to Options.MaxConnAge.
- PoolStats.FreeConns is renamed to PoolStats.IdleConns.
- New hook BeforeSelectQuery.
- `,override` is renamed to `,inherit`.
- Dialer.KeepAlive is set to 5 minutes by default.
- Added support "scram-sha-256" authentication.

## v6.14

- Fields ignored with `sql:"-"` tag are no longer considered by ORM relation detector.

## v6.12

- `Insert`, `Update`, and `Delete` can return `pg.ErrNoRows` and `pg.ErrMultiRows` when `Returning` is used and model expects single row.

## v6.11

- `db.Model(&strct).Update()` and `db.Model(&strct).Delete()` no longer adds WHERE condition based on primary key when there are no conditions. Instead you should use `db.Update(&strct)` or `db.Model(&strct).WherePK().Update()`.

## v6.10

- `?Columns` is renamed to `?TableColumns`. `?Columns` is changed to produce column names without table alias.

## v6.9

- `pg:"fk"` tag now accepts SQL names instead of Go names, e.g. `pg:"fk:ParentId"` becomes `pg:"fk:parent_id"`. Old code should continue working in most cases, but it is strongly advised to start using new convention.
- uint and uint64 SQL type is changed from decimal to bigint according to the the lesser of two evils principle. Use `sql:"type:decimal"` to get old behavior.

## v6.8

- `CreateTable` no longer adds ON DELETE hook by default. To get old behavior users should add `sql:"on_delete:CASCADE"` tag on foreign key field.

## v6

 - `types.Result` is renamed to `orm.Result`.
 - Added `OnQueryProcessed` event that can be used to log / report queries timing. Query logger is removed.
 - `orm.URLValues` is renamed to `orm.URLFilters`. It no longer adds ORDER clause.
 - `orm.Pager` is renamed to `orm.Pagination`.
 - Support for net.IP and net.IPNet.
 - Support for context.Context.
 - Bulk/multi updates.
 - Query.WhereGroup for enclosing conditions in paretheses.

## v5

 - All fields are nullable by default. `,null` tag is replaced with `,notnull`.
 - `Result.Affected` renamed to `Result.RowsAffected`.
 - Added `Result.RowsReturned`.
 - `Create` renamed to `Insert`, `BeforeCreate` to `BeforeInsert`, `AfterCreate` to `AfterInsert`.
 - Indexed placeholders support, e.g. `db.Exec("SELECT ?0 + ?0", 1)`.
 - Named placeholders are evaluated when query is executed.
 - Added Update and Delete hooks.
 - Order reworked to quote column names. OrderExpr added to bypass Order quoting restrictions.
 - Group reworked to quote column names. GroupExpr added to bypass Group quoting restrictions.

## v4

 - `Options.Host` and `Options.Port` merged into `Options.Addr`.
 - Added `Options.MaxRetries`. Now queries are not retried by default.
 - `LoadInto` renamed to `Scan`, `ColumnLoader` renamed to `ColumnScanner`, LoadColumn renamed to ScanColumn, `NewRecord() interface{}` changed to `NewModel() ColumnScanner`, `AppendQuery(dst []byte) []byte` changed to `AppendValue(dst []byte, quote bool) ([]byte, error)`.
 - Structs, maps and slices are marshalled to JSON by default.
 - Added support for scanning slices, .e.g. scanning `[]int`.
 - Added object relational mapping.
