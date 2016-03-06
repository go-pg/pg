# PostgreSQL client for Golang [![Build Status](https://travis-ci.org/go-pg/pg.svg)](https://travis-ci.org/go-pg/pg)

Supports:

- Basic types: integers, floats, string, bool, time.Time.
- sql.NullBool, sql.NullString, sql.NullInt64 and sql.NullFloat64.
- [sql.Scanner](http://golang.org/pkg/database/sql/#Scanner) and [sql/driver.Valuer](http://golang.org/pkg/database/sql/driver/#Valuer) interfaces.
- PostgreSQL Arrays.
- Partially PostgreSQL hstore.
- [JSON](https://godoc.org/gopkg.in/pg.v4#ex-package--Json).
- [Transactions](http://godoc.org/gopkg.in/pg.v4#example-DB-Begin).
- [Prepared statements](http://godoc.org/gopkg.in/pg.v4#example-DB-Prepare).
- [Notifications](http://godoc.org/gopkg.in/pg.v4#example-Listener) using `LISTEN` and `NOTIFY`.
- [Copying data](http://godoc.org/gopkg.in/pg.v4#example-DB-CopyFrom) using `COPY FROM` and `COPY TO`.
- [Timeouts](http://godoc.org/gopkg.in/pg.v4#Options). Client sends `CancelRequest` message on timeout.
- Automatic and safe connection pool.
- Queries retries on network errors.
- Advanced PostgreSQL to Go [struct mapping](http://godoc.org/gopkg.in/pg.v4#example-DB-Query).
- [Migrations](https://github.com/go-pg/migrations).
- [Sharding](https://github.com/go-pg/sharding).

API docs: http://godoc.org/gopkg.in/pg.v4.
Examples: http://godoc.org/gopkg.in/pg.v4#pkg-examples.

## Installation

Install:

    go get gopkg.in/pg.v4

## Quickstart

```go
package pg_test

import (
	"fmt"

	"gopkg.in/pg.v4"
)

type User struct {
	Id     int64
	Name   string
	Emails []string
}

func (u User) String() string {
	return fmt.Sprintf("User<%d %s %v>", u.Id, u.Name, u.Emails)
}

type Story struct {
	Id     int64
	Title  string
	UserId int64
	User   *User
}

func (s Story) String() string {
	return fmt.Sprintf("Story<%d %s %s>", s.Id, s.Title, s.User)
}

func createSchema(db *pg.DB) error {
	queries := []string{
		`CREATE TEMP TABLE users (id serial, name text, emails jsonb)`,
		`CREATE TEMP TABLE stories (id serial, title text, user_id bigint)`,
	}
	for _, q := range queries {
		_, err := db.Exec(q)
		if err != nil {
			return err
		}
	}
	return nil
}

func ExampleDB_Query() {
	db := pg.Connect(&pg.Options{
		User: "postgres",
	})

	err := createSchema(db)
	if err != nil {
		panic(err)
	}

	user1 := &User{
		Name:   "admin",
		Emails: []string{"admin1@admin", "admin2@admin"},
	}
	err = db.Create(user1)
	if err != nil {
		panic(err)
	}

	err = db.Create(&User{
		Name:   "root",
		Emails: []string{"root1@root", "root2@root"},
	})
	if err != nil {
		panic(err)
	}

	story1 := &Story{
		Title:  "Cool story",
		UserId: user1.Id,
	}
	err = db.Create(story1)

	var user User
	err = db.Model(&user).Where("id = ?", user1.Id).Select()
	if err != nil {
		panic(err)
	}

	var users []User
	err = db.Model(&users).Select()
	if err != nil {
		panic(err)
	}

	var story Story
	err = db.Model(&story).
		Columns("stories.*", "User").
		Where("stories.id = ?", story1.Id).
		Select()
	if err != nil {
		panic(err)
	}

	fmt.Println(user)
	fmt.Println(users[0], users[1])
	fmt.Println(story)
	// Output: User<1 admin [admin1@admin admin2@admin]>
	// User<1 admin [admin1@admin admin2@admin]> User<2 root [root1@root root2@root]>
	// Story<1 Cool story User<1 admin [admin1@admin admin2@admin]>>
}
```

## Why not database/sql, lib/pq, or GORM

- No `rows.Close` to manually manage connections.
- go-pg automatically maps rows on Go structs and slice.
- go-pg is at least 3x faster than GORM on querying 100 rows from table.

```
BenchmarkQueryRowsOptimized-4	   10000	    154128 ns/op	   83432 B/op	     625 allocs/op
BenchmarkQueryRowsReflect-4  	   10000	    197921 ns/op	   94760 B/op	     826 allocs/op
BenchmarkQueryRowsORM-4      	   10000	    196123 ns/op	   94992 B/op	     829 allocs/op
BenchmarkQueryRowsStdlibPq-4 	    5000	    255915 ns/op	  161648 B/op	    1324 allocs/op
BenchmarkQueryRowsGORM-4     	    2000	    700051 ns/op	  382501 B/op	    6271 allocs/op
```

- go-pg generates much more effecient queries for joins.

```
BenchmarkQueryHasOneGoPG-4	    3000	    352184 ns/op	   95498 B/op	    1383 allocs/op
BenchmarkQueryHasOneGORM-4	     200	   6887782 ns/op	 2151858 B/op	  113251 allocs/op
```

go-pg queries:

```
SELECT "books".*, "author"."id" AS "author__id", "author"."name" AS "author__name" FROM "books", "authors" AS "author" WHERE "author"."id" = "books"."author_id" LIMIT 100
```

GORM queries:

```
SELECT  * FROM "books"   LIMIT 100
SELECT  * FROM "authors"  WHERE ("id" IN ('1','2'...'100'))
```

## Howto

Please go through [examples](http://godoc.org/gopkg.in/pg.v4#pkg-examples) to get the idea how to use this package.
