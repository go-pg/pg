# Changelog

## v5

 - All fields are nullable by default. `,null` tag is replaced with `,notnull`.
 - `Result.Affected` renamed to `Result.RowsAffected`.
 - Added `Result.RowsReturned`.
 - `Create` renamed to `Insert`, `BeforeCreate` to `BeforeInsert`, `AfterCreate` to `AfterInsert`.
 - Indexed placeholders support, e.g. `db.Exec("SELECT ?0 + ?0", 1)`.
 - Named placeholders are evaluated when query is executed.
 - Added Update and Delete hooks.
 - Order renamed to OrderExpr to indicate that column name is not escaped.

## v4

 - `Options.Host` and `Options.Port` merged into `Options.Addr`.
 - Added `Options.MaxRetries`. Now queries are not retried by default.
 - `LoadInto` renamed to `Scan`, `ColumnLoader` renamed to `ColumnScanner`, LoadColumn renamed to ScanColumn, `NewRecord() interface{}` changed to `NewModel() ColumnScanner`, `AppendQuery(dst []byte) []byte` changed to `AppendValue(dst []byte, quote bool) ([]byte, error)`.
 - Structs, maps and slices are marshalled to JSON by default.
 - Added support for scanning slices, .e.g. scanning `[]int`.
 - Added object relational mapping.
