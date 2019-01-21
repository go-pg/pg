# PostgreSQL client and ORM for Golang

[![Build Status](https://travis-ci.org/go-pg/pg.svg?branch=master)](https://travis-ci.org/go-pg/pg)
[![GoDoc](https://godoc.org/github.com/go-pg/pg?status.svg)](https://godoc.org/github.com/go-pg/pg)

## Features:

- Basic types: integers, floats, string, bool, time.Time, net.IP, net.IPNet.
- sql.NullBool, sql.NullString, sql.NullInt64, sql.NullFloat64 and [pg.NullTime](http://godoc.org/github.com/go-pg/pg#NullTime).
- [sql.Scanner](http://golang.org/pkg/database/sql/#Scanner) and [sql/driver.Valuer](http://golang.org/pkg/database/sql/driver/#Valuer) interfaces.
- Structs, maps and arrays are marshalled as JSON by default.
- PostgreSQL multidimensional Arrays using [array tag](https://godoc.org/github.com/go-pg/pg#example-DB-Model-PostgresArrayStructTag) and [Array wrapper](https://godoc.org/github.com/go-pg/pg#example-Array).
- Hstore using [hstore tag](https://godoc.org/github.com/go-pg/pg#example-DB-Model-HstoreStructTag) and [Hstore wrapper](https://godoc.org/github.com/go-pg/pg#example-Hstore).
- [Composite types](https://godoc.org/github.com/go-pg/pg#example-DB-Model-CompositeType).
- All struct fields are nullable by default and zero values (empty string, 0, zero time, empty map or slice) are marshalled as SQL `NULL`. `sql:",notnull"` tag is used to reverse this behaviour.
- [Transactions](http://godoc.org/github.com/go-pg/pg#example-DB-Begin).
- [Prepared statements](http://godoc.org/github.com/go-pg/pg#example-DB-Prepare).
- [Notifications](http://godoc.org/github.com/go-pg/pg#example-Listener) using `LISTEN` and `NOTIFY`.
- [Copying data](http://godoc.org/github.com/go-pg/pg#example-DB-CopyFrom) using `COPY FROM` and `COPY TO`.
- [Timeouts](http://godoc.org/github.com/go-pg/pg#Options).
- Automatic connection pooling with [circuit breaker](https://en.wikipedia.org/wiki/Circuit_breaker_design_pattern) support.
- Queries retries on network errors.
- Working with models using [ORM](https://godoc.org/github.com/go-pg/pg#example-DB-Model) and [SQL](https://godoc.org/github.com/go-pg/pg#example-DB-Query).
- Scanning variables using [ORM](https://godoc.org/github.com/go-pg/pg#example-DB-Select-SomeColumnsIntoVars) and [SQL](https://godoc.org/github.com/go-pg/pg#example-Scan).
- [SelectOrInsert](https://godoc.org/github.com/go-pg/pg#example-DB-Insert-SelectOrInsert) using on-conflict.
- [INSERT ... ON CONFLICT DO UPDATE](https://godoc.org/github.com/go-pg/pg#example-DB-Insert-OnConflictDoUpdate) using ORM.
- Bulk/batch [inserts](https://godoc.org/github.com/go-pg/pg#example-DB-Insert-BulkInsert), [updates](https://godoc.org/github.com/go-pg/pg#example-DB-Update-BulkUpdate), and [deletes](https://godoc.org/github.com/go-pg/pg#example-DB-Delete-BulkDelete).
- Common table expressions using [WITH](https://godoc.org/github.com/go-pg/pg#example-DB-Select-With) and [WrapWith](https://godoc.org/github.com/go-pg/pg#example-DB-Select-WrapWith).
- [CountEstimate](https://godoc.org/github.com/go-pg/pg#example-DB-Model-CountEstimate) using `EXPLAIN` to get [estimated number of matching rows](https://wiki.postgresql.org/wiki/Count_estimate).
- ORM supports [has one](https://godoc.org/github.com/go-pg/pg#example-DB-Model-HasOne), [belongs to](https://godoc.org/github.com/go-pg/pg#example-DB-Model-BelongsTo), [has many](https://godoc.org/github.com/go-pg/pg#example-DB-Model-HasMany), and [many to many](https://godoc.org/github.com/go-pg/pg#example-DB-Model-ManyToMany) with composite/multi-column primary keys.
- [Soft deletes](https://godoc.org/github.com/go-pg/pg#example-DB-Model-SoftDelete).
- [Creating tables from structs](https://godoc.org/github.com/go-pg/pg#example-DB-CreateTable).
- [Pagination](https://godoc.org/github.com/go-pg/pg/urlvalues#NewPager) and [URL filters](https://godoc.org/github.com/go-pg/pg/urlvalues#Filters) helpers.
- [ForEach](https://godoc.org/github.com/go-pg/pg#example-DB-Model-ForEach) that calls a function for each row returned by the query without loading all rows into the memory.
- Works with PgBouncer in transaction pooling mode.
- [Migrations](https://github.com/go-pg/migrations).
- [Sharding](https://github.com/go-pg/sharding).
- [Model generator from SQL tables](https://github.com/dizzyfool/genna).

## Get Started

```shell
go get -u github.com/go-pg/pg
```

- [Wiki](https://github.com/go-pg/pg/wiki)
- [API docs](http://godoc.org/github.com/go-pg/pg)
- [Examples](http://godoc.org/github.com/go-pg/pg#pkg-examples)

## Look & Feel

```go
package pg_test

import (
    "fmt"

    "github.com/go-pg/pg"
    "github.com/go-pg/pg/orm"
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
    Id       int64
    Title    string
    AuthorId int64
    Author   *User
}

func (s Story) String() string {
    return fmt.Sprintf("Story<%d %s %s>", s.Id, s.Title, s.Author)
}

func ExampleDB_Model() {
    db := pg.Connect(&pg.Options{
        User: "postgres",
    })
    defer db.Close()

    err := createSchema(db)
    if err != nil {
        panic(err)
    }

    user1 := &User{
        Name:   "admin",
        Emails: []string{"admin1@admin", "admin2@admin"},
    }
    err = db.Insert(user1)
    if err != nil {
        panic(err)
    }

    err = db.Insert(&User{
        Name:   "root",
        Emails: []string{"root1@root", "root2@root"},
    })
    if err != nil {
        panic(err)
    }

    story1 := &Story{
        Title:    "Cool story",
        AuthorId: user1.Id,
    }
    err = db.Insert(story1)
    if err != nil {
        panic(err)
    }

    // Select user by primary key.
    user := &User{Id: user1.Id}
    err = db.Select(user)
    if err != nil {
        panic(err)
    }

    // Select all users.
    var users []User
    err = db.Model(&users).Select()
    if err != nil {
        panic(err)
    }

    // Select story and associated author in one query.
    story := new(Story)
    err = db.Model(story).
        Relation("Author").
        Where("story.id = ?", story1.Id).
        Select()
    if err != nil {
        panic(err)
    }

    fmt.Println(user)
    fmt.Println(users)
    fmt.Println(story)
    // Output: User<1 admin [admin1@admin admin2@admin]>
    // [User<1 admin [admin1@admin admin2@admin]> User<2 root [root1@root root2@root]>]
    // Story<1 Cool story User<1 admin [admin1@admin admin2@admin]>>
}

func createSchema(db *pg.DB) error {
    for _, model := range []interface{}{(*User)(nil), (*Story)(nil)} {
        err := db.CreateTable(model, &orm.CreateTableOptions{
            Temp: true,
        })
        if err != nil {
            return err
        }
    }
    return nil
}
```

## See also

- [Golang msgpack](https://github.com/vmihailenco/msgpack)
- [Golang message task queue](https://github.com/go-msgqueue/msgqueue)
