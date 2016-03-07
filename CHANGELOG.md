# Changelog

## v4

- `LoadInto` renamed to `Scan`, `ColumnLoader` renamed to `ColumnScanner`, LoadColumn renamed to ScanColumn, `NewRecord() interface{}` changed to `NewModel() ColumnScanner`, `AppendQuery(dst []byte) []byte` changed to `AppendValue(dst []byte, quote bool) ([]byte, error)`.
- Structs, maps and slices are marshalled to JSON by default.
- Added support for scanning slices, .e.g. scanning `[]int`.
- Added object relational mapping.
