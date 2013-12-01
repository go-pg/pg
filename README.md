PostgreSQL client for Golang
============================

Supports:

- Arrays.
- Partially hstore.
- Transactions.
- Prepared statements.
- Notifications, LISTEN/NOTIFY.
- Timeouts. Client sends `CancelRequest` message on timeout.
- Connection pool.
- PostgreSQL to Go struct mapping.

API docs: http://godoc.org/github.com/vmihailenco/pg. Make sure to check examples: http://godoc.org/github.com/vmihailenco/pg#pkg-examples.

Installation
------------

Install:

    go get github.com/vmihailenco/pg

Example
-------

    package pg_test

    import (
        "fmt"

        "github.com/vmihailenco/pg"
    )

    type User struct {
        Name   string
        Emails []string
    }

    type Users struct {
        Values []*User
    }

    func (f *Users) New() interface{} {
        u := &User{}
        f.Values = append(f.Values, u)
        return u
    }

    func ExampleDB_Query() {
        db := pg.Connect(&pg.Options{
            User: "postgres",
        })
        defer db.Close()

        users := &Users{}
        res, err := db.Query(users,
            `WITH users (name, emails) AS (VALUES (?, ?), (?, ?))
            SELECT * FROM users`,
            "admin", []string{"admin1@admin", "admin2@admin"},
            "root", []string{"root1@root", "root2@root"},
        )
        fmt.Println(res.Affected(), err)
        fmt.Println(users.Values[0], users.Values[1])
        // Output: 2 <nil>
        // &{admin [admin1@admin admin2@admin]} &{root [root1@root root2@root]}
    }
