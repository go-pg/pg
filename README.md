# PostgreSQL client for Golang [![Build Status](https://travis-ci.org/go-pg/pg.svg)](https://travis-ci.org/go-pg/pg)

## Features:

- Basic types: integers, floats, string, bool, time.Time.
- sql.NullBool, sql.NullString, sql.NullInt64, sql.NullFloat64 and [pg.NullTime](http://godoc.org/gopkg.in/pg.v5#NullTime).
- [sql.Scanner](http://golang.org/pkg/database/sql/#Scanner) and [sql/driver.Valuer](http://golang.org/pkg/database/sql/driver/#Valuer) interfaces.
- Structs, maps and arrays are marshalled as JSON by default.
- PostgreSQL multidimensional Arrays using [array tag](https://godoc.org/gopkg.in/pg.v5#example-DB-Model-PostgresArrayStructTag) and [Array wrapper](https://godoc.org/gopkg.in/pg.v5#example-Array).
- Hstore using [hstore tag](https://godoc.org/gopkg.in/pg.v5#example-DB-Model-HstoreStructTag) and [Hstore wrapper](https://godoc.org/gopkg.in/pg.v5#example-Hstore).
- All struct fields are nullable by default and zero values (empty string, 0, zero time) are marshalled as SQL `NULL`. ```sql:",notnull"` is used to reverse this behaviour.
- [Transactions](http://godoc.org/gopkg.in/pg.v5#example-DB-Begin).
- [Prepared statements](http://godoc.org/gopkg.in/pg.v5#example-DB-Prepare).
- [Notifications](http://godoc.org/gopkg.in/pg.v5#example-Listener) using `LISTEN` and `NOTIFY`.
- [Copying data](http://godoc.org/gopkg.in/pg.v5#example-DB-CopyFrom) using `COPY FROM` and `COPY TO`.
- [Timeouts](http://godoc.org/gopkg.in/pg.v5#Options).
- Automatic connection pooling.
- Queries retries on network errors.
- Working with models using [ORM](https://godoc.org/gopkg.in/pg.v5#example-DB-Model) and [SQL](https://godoc.org/gopkg.in/pg.v5#example-DB-Query).
- Scanning variables using [ORM](https://godoc.org/gopkg.in/pg.v5#example-DB-Select-SomeColumnsIntoVars) and [SQL](https://godoc.org/gopkg.in/pg.v5#example-Scan).
- [SelectOrInsert](https://godoc.org/gopkg.in/pg.v5#example-DB-Insert-SelectOrInsert) using on-conflict.
- [INSERT ... ON CONFLICT DO UPDATE](https://godoc.org/gopkg.in/pg.v5#example-DB-Insert-OnConflictDoUpdate) using ORM.
- Common table expressions using [WITH](https://godoc.org/gopkg.in/pg.v5#example-DB-Select-With) and [WrapWith](https://godoc.org/gopkg.in/pg.v5#example-DB-Select-WrapWith).
- [CountEstimate](https://godoc.org/gopkg.in/pg.v5#example-DB-Model-CountEstimate) using `EXPLAIN` to get [estimated number of matching rows](https://wiki.postgresql.org/wiki/Count_estimate).
- [HasOne](https://godoc.org/gopkg.in/pg.v5#example-DB-Model-HasOne), [BelongsTo](https://godoc.org/gopkg.in/pg.v5#example-DB-Model-BelongsTo), [HasMany](https://godoc.org/gopkg.in/pg.v5#example-DB-Model-HasMany) and [ManyToMany](https://godoc.org/gopkg.in/pg.v5#example-DB-Model-ManyToMany).
- [Creating tables from structs](https://godoc.org/gopkg.in/pg.v5#example-DB-CreateTable).
- [Migrations](https://github.com/go-pg/migrations).
- [Sharding](https://github.com/go-pg/sharding).

## Get Started
- [Wiki](https://github.com/go-pg/pg/wiki)   
- [API docs](http://godoc.org/gopkg.in/pg.v5) 
- [Examples](http://godoc.org/gopkg.in/pg.v5#pkg-examples)

