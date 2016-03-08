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

# Table of contents

* [Installation](#installation)
* [Quickstart](#quickstart)
* [Why go-pg](#why-go-pg)
* [Howto](#howto)
* [ORM](#orm)
  * [Model definition](#model-definition)
  * [Select model](#select-model)
  * [Update specified columns](#update-specified-columns)
  * [Delete multiple models](#delete-multiple-models)
  * [Count rows](#count-rows)

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
		Columns("story.*", "User").
		Where("story.id = ?", story1.Id).
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

## Why go-pg

- No `rows.Close` to manually manage connections.
- go-pg automatically maps rows on Go structs and slice.
- go-pg is at least 3x faster than GORM on querying 100 rows from table.

    ```
    BenchmarkQueryRowsGopgOptimized-4       	   10000	    142209 ns/op	   83432 B/op	     625 allocs/op
    BenchmarkQueryRowsGopgReflect-4         	   10000	    173866 ns/op	   94760 B/op	     826 allocs/op
    BenchmarkQueryRowsGopgORM-4             	   10000	    173000 ns/op	   95024 B/op	     830 allocs/op
    BenchmarkQueryRowsStdlibPq-4            	    5000	    222594 ns/op	  161648 B/op	    1324 allocs/op
    BenchmarkQueryRowsGORM-4                	    2000	    629088 ns/op	  392950 B/op	    6667 allocs/op
    ```

- go-pg generates much more effecient queries for joins.

    ```
    BenchmarkQueryHasOneGopg-4              	    3000	    344230 ns/op	   92310 B/op	    1384 allocs/op
    BenchmarkQueryHasOneGORM-4              	     300	   5667656 ns/op	 2041169 B/op	  103467 allocs/op
    ```

    go-pg:

    ```go
    db.Model(&books).Columns("book.*", "Author").Limit(100).Select()
    ```

    ```sql
    SELECT "book".*, "author"."id" AS "author__id", "author"."name" AS "author__name"
    FROM "books" AS "book", "authors" AS "author"
    WHERE "author"."id" = "books"."author_id"
    LIMIT 100
    ```

    GORM:

    ```go
    db.Preload("Author").Limit(100).Find(&books).Error
    ```

    ```sql
    SELECT  * FROM "books"   LIMIT 100
    SELECT  * FROM "authors"  WHERE ("id" IN ('1','2'...'100'))
    ```

## Howto

Please go through [examples](http://godoc.org/gopkg.in/pg.v4#pkg-examples) to get the idea how to use this package.

## ORM

### Model definition

```go
type Genre struct {
	Id     int // Id is automatically detected as primary key
	Name   string
	Rating int `sql:"-"` // - is used to ignore field

	Books      []Book      `pg:",many2many:BookGenres"` // many to many relation
	BookGenres []BookGenre // join model for many to many relation
}

type Author struct {
	ID    int // both "Id" and "ID" are detected as primary key
	Name  string
	Books []Book // has many relation
}

type BookGenre struct {
	BookId  int `sql:",pk"` // pk tag is used to mark field as primary key
	GenreId int `sql:",pk"`

	GenreRating int // belongs to and is copied to Genre model
}

type Book struct {
	Id        int
	Title     string
	AuthorID  int
	Author    *Author // has one relation
	EditorID  int
	Editor    *Author // has one relation
	CreatedAt time.Time

	Genres     []Genre     `pg:",many2many:BookGenres"` // many to many relation
	BookGenres []BookGenre // join model for many to many relation

	Translations []Translation // has many relation

	Comments []Comment `pg:",polymorphic:Trackable"` // has many polymorphic relation
}

type Translation struct {
	TableName struct{} `sql:"book_translations"` // specifies custom table name

	Id     int
	BookId int
	Book   *Book // belongs to relation
	Lang   string

	Comments []Comment `pg:",polymorphic:Trackable"` // has many polymorphic relation
}

type Comment struct {
	TrackableId   int    `sql:",pk"` // can be Book.Id or Translation.Id
	TrackableType string `sql:",pk"` // can be "book" or "translation"
	Text          string
}
```

### Select model

```go
var book Book
err := db.Model(&book).
	Columns("book.*", "Author", "Editor", "Genres", "Comments", "Translations", "Translations.Comments").
	Order("book.id DESC").
	Limit(1).
	Select()

var books []Book
err := db.Model(&book).
	Columns("book.*", "Author", "Editor", "Genres", "Comments", "Translations", "Translations.Comments").
	Order("book.id DESC").
	Limit(10).
	Select()
```

### Create, update, and delete model

```go
book := Book{
	Title:     "book 1",
	AuthorID:  10,
	EditorID:  11,
	CreatedAt: time.Now(),
}
err := db.Create(&book)

err = db.Update(book)

err = db.Delete(book)
```

### Update specified columns

```go
id := 100
data := map[string]interface{}{
	"title": pg.Q("concat(?, title, ?)", "prefix ", " suffix"),
}

var book Book
err := db.Model(&book).
	Where("id = ?", id).
	Returning("*").
	UpdateValues(data)
```

### Delete multiple models

```go
ids := pg.Ints{100, 101}
err := db.Model(&Book{}).Where("id IN (?)", ids).Delete()
```

### Count rows

```go
var count int
err := db.Model(&Book{}).Count(&count)
```
