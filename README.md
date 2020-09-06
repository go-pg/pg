# PostgreSQL client and ORM for Golang

[![Build Status](https://travis-ci.org/go-pg/pg.svg?branch=v10)](https://travis-ci.org/go-pg/pg)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/go-pg/pg/v10)](https://pkg.go.dev/github.com/go-pg/pg/v10)
[![Documentation](https://img.shields.io/badge/pg-documentation-informational)](https://pg.uptrace.dev/)

> :heart: [**Uptrace.dev** - distributed traces, logs, and errors in one place](https://uptrace.dev)

- [Docs](https://pg.uptrace.dev)
- [Reference](https://pkg.go.dev/github.com/go-pg/pg/v10?tab=doc)
- [Examples](https://pkg.go.dev/github.com/go-pg/pg/v10?tab=doc#pkg-examples)
- [RealWorld example app](https://github.com/uptrace/go-realworld-example-app)
- Use [Discord](https://discord.gg/SKUFEz) or [stackoverflow](https://stackoverflow.com/) to ask
  questions.

## Ecosystem

- [pgext](https://github.com/go-pg/pgext) - faster JSON encoding, OpenTelemetry hook, etc.
- Migrations by [vmihailenco](https://github.com/go-pg/migrations) and
  [robinjoseph08](https://github.com/robinjoseph08/go-pg-migrations).
- [Sharding](https://github.com/go-pg/sharding).
- [Model generator from SQL tables](https://github.com/dizzyfool/genna).
- [urlstruct](https://github.com/go-pg/urlstruct) to decode `url.Values` into structs.

## Features

- Basic types: integers, floats, string, bool, time.Time, net.IP, net.IPNet.
- sql.NullBool, sql.NullString, sql.NullInt64, sql.NullFloat64 and
  [pg.NullTime](https://pkg.go.dev/github.com/go-pg/pg/v10?tab=doc#NullTime).
- [sql.Scanner](http://golang.org/pkg/database/sql/#Scanner) and
  [sql/driver.Valuer](http://golang.org/pkg/database/sql/driver/#Valuer) interfaces.
- Structs, maps and arrays are marshalled as JSON by default.
- PostgreSQL multidimensional Arrays using
  [array tag](https://pkg.go.dev/github.com/go-pg/pg/v10?tab=doc#example-DB-Model-PostgresArrayStructTag)
  and [Array wrapper](https://pkg.go.dev/github.com/go-pg/pg/v10?tab=doc#example-Array).
- Hstore using
  [hstore tag](https://pkg.go.dev/github.com/go-pg/pg/v10?tab=doc#example-DB-Model-HstoreStructTag)
  and [Hstore wrapper](https://pkg.go.dev/github.com/go-pg/pg/v10?tab=doc#example-Hstore).
- [Composite types](https://pkg.go.dev/github.com/go-pg/pg/v10?tab=doc#example-DB-Model-CompositeType).
- All struct fields are nullable by default and zero values (empty string, 0, zero time, empty map
  or slice, nil ptr) are marshalled as SQL `NULL`. `pg:",notnull"` is used to add SQL `NOT NULL`
  constraint and `pg:",use_zero"` to allow Go zero values.
- [Transactions](https://pkg.go.dev/github.com/go-pg/pg/v10?tab=doc#example-DB-Begin).
- [Prepared statements](https://pkg.go.dev/github.com/go-pg/pg/v10?tab=doc#example-DB-Prepare).
- [Notifications](https://pkg.go.dev/github.com/go-pg/pg/v10?tab=doc#example-Listener) using
  `LISTEN` and `NOTIFY`.
- [Copying data](https://pkg.go.dev/github.com/go-pg/pg/v10?tab=doc#example-DB-CopyFrom) using
  `COPY FROM` and `COPY TO`.
- [Timeouts](https://pkg.go.dev/github.com/go-pg/pg/v10?tab=doc#Options) and canceling queries using
  context.Context.
- Automatic connection pooling with
  [circuit breaker](https://en.wikipedia.org/wiki/Circuit_breaker_design_pattern) support.
- Queries retry on network errors.
- Working with models using
  [ORM](https://pkg.go.dev/github.com/go-pg/pg/v10?tab=doc#example-DB-Model) and
  [SQL](https://pkg.go.dev/github.com/go-pg/pg/v10?tab=doc#example-DB-Query).
- Scanning variables using
  [ORM](https://pkg.go.dev/github.com/go-pg/pg/v10?tab=doc#example-DB-Select-SomeColumnsIntoVars)
  and [SQL](https://pkg.go.dev/github.com/go-pg/pg/v10?tab=doc#example-Scan).
- [SelectOrInsert](https://pkg.go.dev/github.com/go-pg/pg/v10?tab=doc#example-DB-Insert-SelectOrInsert)
  using on-conflict.
- [INSERT ... ON CONFLICT DO UPDATE](https://pkg.go.dev/github.com/go-pg/pg/v10?tab=doc#example-DB-Insert-OnConflictDoUpdate)
  using ORM.
- Bulk/batch
  [inserts](https://pkg.go.dev/github.com/go-pg/pg/v10?tab=doc#example-DB-Insert-BulkInsert),
  [updates](https://pkg.go.dev/github.com/go-pg/pg/v10?tab=doc#example-DB-Update-BulkUpdate), and
  [deletes](https://pkg.go.dev/github.com/go-pg/pg/v10?tab=doc#example-DB-Delete-BulkDelete).
- Common table expressions using
  [WITH](https://pkg.go.dev/github.com/go-pg/pg/v10?tab=doc#example-DB-Select-With) and
  [WrapWith](https://pkg.go.dev/github.com/go-pg/pg/v10?tab=doc#example-DB-Select-WrapWith).
- [CountEstimate](https://pkg.go.dev/github.com/go-pg/pg/v10?tab=doc#example-DB-Model-CountEstimate)
  using `EXPLAIN` to get
  [estimated number of matching rows](https://wiki.postgresql.org/wiki/Count_estimate).
- ORM supports
  [has one](https://pkg.go.dev/github.com/go-pg/pg/v10?tab=doc#example-DB-Model-HasOne),
  [belongs to](https://pkg.go.dev/github.com/go-pg/pg/v10?tab=doc#example-DB-Model-BelongsTo),
  [has many](https://pkg.go.dev/github.com/go-pg/pg/v10?tab=doc#example-DB-Model-HasMany), and
  [many to many](https://pkg.go.dev/github.com/go-pg/pg/v10?tab=doc#example-DB-Model-ManyToMany)
  with composite/multi-column primary keys.
- [Soft deletes](https://pkg.go.dev/github.com/go-pg/pg/v10?tab=doc#example-DB-Model-SoftDelete).
- [Creating tables from structs](https://pkg.go.dev/github.com/go-pg/pg/v10?tab=doc#example-DB-CreateTable).
- [ForEach](https://pkg.go.dev/github.com/go-pg/pg/v10?tab=doc#example-DB-Model-ForEach) that calls
  a function for each row returned by the query without loading all rows into the memory.
- Works with PgBouncer in transaction pooling mode.

## See also

- [Golang msgpack](https://github.com/vmihailenco/msgpack)
- [Golang message task queue](https://github.com/vmihailenco/taskq)
