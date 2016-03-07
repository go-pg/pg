# Changelog

## v4

- LoadInto renamed to Scan, ColumnLoader renamed to ColumnScanner, LoadColumn renamed to ScanColumn, `NewRecord() interface{}` changed to `NewModel() ColumnScanner`.
- Structs, maps and slices are marshalled to JSON by default.
- Added support for scanning slices, .e.g. scanning `[]int`.
- Added object relational mapping.
