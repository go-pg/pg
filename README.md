# PostgreSQL client for Golang [![Build Status](https://travis-ci.org/go-pg/pg.svg)](https://travis-ci.org/go-pg/pg)

Supports:

- Basic types: integers, floats, string, bool, time.Time, and pointers to these types.
- sql.NullBool, sql.NullString, sql.NullInt64 and sql.Float64.
- [sql.Scanner](http://golang.org/pkg/database/sql/#Scanner) and [sql/driver.Valuer](http://golang.org/pkg/database/sql/driver/#Valuer) interfaces.
- Arrays.
- Partially hstore.
- [JSON](https://godoc.org/gopkg.in/pg.v3#ex-package--Json).
- [Transactions](http://godoc.org/gopkg.in/pg.v3#example-DB-Begin).
- [Prepared statements](http://godoc.org/gopkg.in/pg.v3#example-DB-Prepare).
- [Notifications](http://godoc.org/gopkg.in/pg.v3#example-Listener) using `LISTEN` and `NOTIFY`.
- [Copying data](http://godoc.org/gopkg.in/pg.v3#example-DB-CopyFrom) using `COPY FROM` and `COPY TO`.
- [Timeouts](http://godoc.org/gopkg.in/pg.v3#Options). Client sends `CancelRequest` message on timeout.
- Automatic and safe connection pool.
- Queries retries on network errors.
- PostgreSQL to Go [struct mapping](http://godoc.org/gopkg.in/pg.v3#example-DB-Query).
- [Migrations](https://github.com/go-pg/migrations).
- [Sharding](https://github.com/go-pg/sharding).

API docs: http://godoc.org/gopkg.in/pg.v3.
Examples: http://godoc.org/gopkg.in/pg.v3#pkg-examples.

## Installation

Install:

    go get gopkg.in/pg.v3

## Quickstart

```go
package pg_test

import (
	"fmt"

	"gopkg.in/pg.v3"
)

type User struct {
	Id     int64
	Name   string
	Emails []string
}

func CreateUser(db *pg.DB, user *User) error {
	_, err := db.QueryOne(user, `
		INSERT INTO users (name, emails) VALUES (?name, ?emails)
		RETURNING id
	`, user)
	return err
}

func GetUser(db *pg.DB, id int64) (*User, error) {
	var user User
	_, err := db.QueryOne(&user, `SELECT * FROM users WHERE id = ?`, id)
	return &user, err
}

func GetUsers(db *pg.DB) ([]User, error) {
	var users []User
	_, err := db.Query(&users, `SELECT * FROM users`)
	return users, err
}

func GetUsersByIds(db *pg.DB, ids []int64) ([]User, error) {
	var users []User
	_, err := db.Query(&users, `SELECT * FROM users WHERE id IN (?)`, pg.Ints(ids))
	return users, err
}

func ExampleDB_Query() {
	db := pg.Connect(&pg.Options{
		User: "postgres",
	})

	_, err := db.Exec(`CREATE TEMP TABLE users (id serial, name text, emails text[])`)
	if err != nil {
		panic(err)
	}

	err = CreateUser(db, &User{
		Name:   "admin",
		Emails: []string{"admin1@admin", "admin2@admin"},
	})
	if err != nil {
		panic(err)
	}

	err = CreateUser(db, &User{
		Name:   "root",
		Emails: []string{"root1@root", "root2@root"},
	})
	if err != nil {
		panic(err)
	}

	user, err := GetUser(db, 1)
	if err != nil {
		panic(err)
	}

	users, err := GetUsers(db)
	if err != nil {
		panic(err)
	}

	fmt.Println(user)
	fmt.Println(users[0], users[1])
	// Output: &{1 admin [admin1@admin admin2@admin]}
	// {1 admin [admin1@admin admin2@admin]} {2 root [root1@root root2@root]}
}
```

## Why not database/sql, lib/pq, or GORM

- No `rows.Close` to manually manage connections.
- go-pg can automatically map rows on Go structs.
- go-pg is at least 3x faster than GORM on querying 100 rows from table.
- go-pg supports client-side placeholders that allow you to write [complex queries](https://godoc.org/gopkg.in/pg.v3#example-package--ComplexQuery) and have full power of SQL.

## Benchmark

```
BenchmarkQueryRowsOptimized-4	   10000	    154480 ns/op	   87789 B/op	     624 allocs/op
BenchmarkQueryRowsReflect-4  	   10000	    196261 ns/op	  102224 B/op	     925 allocs/op
BenchmarkQueryRowsStdlibPq-4 	    5000	    236584 ns/op	  166528 B/op	    1324 allocs/op
BenchmarkQueryRowsGORM-4     	    2000	    690532 ns/op	  399661 B/op	    6171 allocs/op
```

## Howto

Please go through [examples](http://godoc.org/gopkg.in/pg.v3#pkg-examples) to get the idea how to use this package.
