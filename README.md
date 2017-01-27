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

## Look & Feel

```go
package pg_test

import (
   "fmt"

   "gopkg.in/pg.v5"
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
   user := User{Id: user1.Id}
   err = db.Select(&user)
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
   var story Story
   err = db.Model(&story).
      Column("story.*", "Author").
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
   queries := []string{
      `CREATE TEMP TABLE users (id serial, name text, emails jsonb)`,
      `CREATE TEMP TABLE stories (id serial, title text, author_id bigint)`,
   }
   for _, q := range queries {
      _, err := db.Exec(q)
      if err != nil {
         return err
      }
   }
   return nil
}
```
