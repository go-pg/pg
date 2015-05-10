PostgreSQL client for Golang [![Build Status](https://travis-ci.org/go-pg/pg.svg)](https://travis-ci.org/go-pg/pg)
===

Supports:

- Basic types: integers, floats, string, bool, time.Time, and pointers to these types.
- sql.NullBool, sql.NullString, sql.NullInt64 and sql.Float64.
- [sql.Scanner](http://golang.org/pkg/database/sql/#Scanner) and [sql/driver.Valuer](http://golang.org/pkg/database/sql/driver/#Valuer) interfaces.
- Arrays.
- Partially hstore.
- Transactions.
- Prepared statements.
- Notifications: `LISTEN`/`NOTIFY`.
- `COPY FROM` and `COPY TO`.
- Timeouts. Client sends `CancelRequest` message on timeout.
- Connection pool.
- Queries are retried when possible.
- PostgreSQL to Go struct mapping.

API docs: http://godoc.org/gopkg.in/pg.v3.

Installation
------------

Install:

    go get gopkg.in/pg.v3

Quickstart
----------

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

type Users struct {
	C []User
}

var _ pg.Collection = &Users{}

func (users *Users) NewRecord() interface{} {
	users.C = append(users.C, User{})
	return &users.C[len(users.C)-1]
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
	var users Users
	_, err := db.Query(&users, `SELECT * FROM users`)
	return users.C, err
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

Howto
-----

Please go through [examples](http://godoc.org/gopkg.in/pg.v3#pkg-examples) to get the idea how to use this package.

Changelog
---------

### v3 (beta)

* `pg.Factory` renamed to `pg.Collection`. `Factory.New` renamed to `Collection.NewRecord`.
* `pg.Loader` renamed to `pg.ColumnLoader`. `ColumnLoader.Load` renamed to `ColumnLoader.LoadColumn`.
* `pg.Appender` renamed to `pg.QueryAppender`. `QueryAppender.Append` renamed to `QueryAppender.AppendQuery`.

### v2

* Support for named placeholders:

```go
a := &Article{Id: 1, Name: "Hello world"}
_, err := db.ExecOne(`UPDATE articles SET name = ?name WHERE id = ?id`, a)
```

* CopyFrom/CopyTo support:

```go
r := strings.NewReader("hello\t5\nworld\t5\nfoo\t3\nbar\t3\n")
res, err := t.db.CopyFrom(r, "COPY test FROM STDIN")
```

* Simplify collections:

```go
type Articles []*Article

func (articles *Articles) New() interface{} {
    a := &Article{}
    *articles = append(*articles, a)
    return a
}
```

### v1

Initial release.
