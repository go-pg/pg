# Changelog

## v4

- `Options.Host` and `Options.Port` merged into `Options.Addr`.
- `Options.IdleCheckFrequency` is removed.
- Added `Options.MaxRetries`. Now queries are not retried by default.
- `LoadInto` renamed to `Scan`, `ColumnLoader` renamed to `ColumnScanner`, LoadColumn renamed to ScanColumn, `NewRecord() interface{}` changed to `NewModel() ColumnScanner`, `AppendQuery(dst []byte) []byte` changed to `AppendValue(dst []byte, quote bool) ([]byte, error)`.
- Structs, maps and slices are marshalled to JSON by default.
- Added support for scanning slices, .e.g. scanning `[]int`.
- Added object relational mapping.
