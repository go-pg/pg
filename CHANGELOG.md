# Changelog

> :heart: [**Uptrace.dev** - distributed traces, logs, and errors in one place](https://uptrace.dev)

See https://pg.uptrace.dev/changelog/

## v11

- Most API `context.Context` accepts `context.Context` as a first arg.
- `Read/WriteTimeout` is enabled by default.
- Added `Query.ModelTableExpr` to overwrite model table name.
- Added `Query.WithValues` that generates `VALUES` query.
- Renamed `With` to `WithSelect`.
- Function passed to `Query.Apply` does not return an error any more. Use `q.Err(err)` instead.
