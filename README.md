# PostgreSQL client for Golang [![Build Status](https://travis-ci.org/go-pg/pg.svg)](https://travis-ci.org/go-pg/pg)

Supports:

- Basic types: integers, floats, string, bool, time.Time.
- sql.NullBool, sql.NullString, sql.NullInt64 and sql.NullFloat64.
- `sql:",null"` struct tag which marshalls zero struct fields as SQL `NULL` and completely omits them from `INSERT` queries.
- [sql.Scanner](http://golang.org/pkg/database/sql/#Scanner) and [sql/driver.Valuer](http://golang.org/pkg/database/sql/driver/#Valuer) interfaces.
- Structs, maps and arrays are marshalled as JSON by default.
- PostgreSQL Arrays using [array tag](https://godoc.org/gopkg.in/pg.v4#example-DB-Model-PostgresArrayStructTag) and [Array wrapper](https://godoc.org/gopkg.in/pg.v4#example-Array).
- [Transactions](http://godoc.org/gopkg.in/pg.v4#example-DB-Begin).
- [Prepared statements](http://godoc.org/gopkg.in/pg.v4#example-DB-Prepare).
- [Notifications](http://godoc.org/gopkg.in/pg.v4#example-Listener) using `LISTEN` and `NOTIFY`.
- [Copying data](http://godoc.org/gopkg.in/pg.v4#example-DB-CopyFrom) using `COPY FROM` and `COPY TO`.
- [Timeouts](http://godoc.org/gopkg.in/pg.v4#Options).
- Automatic connection pooling.
- Queries retries on network errors.
- Working with models using [ORM](https://godoc.org/gopkg.in/pg.v4#example-DB-Model) and [SQL](https://godoc.org/gopkg.in/pg.v4#example-DB-Query).
- Scanning variables using [ORM](https://godoc.org/gopkg.in/pg.v4#example-DB-Select-SomeColumnsIntoVars) and [SQL](https://godoc.org/gopkg.in/pg.v4#example-Scan).
- [SelectOrCreate](https://godoc.org/gopkg.in/pg.v4#example-DB-Create-SelectOrCreate) using upserts.
- [CountEstimate](https://godoc.org/gopkg.in/pg.v4#example-DB-Model-CountEstimate) using `EXPLAIN` to get [estimated number of matching rows](https://wiki.postgresql.org/wiki/Count_estimate).
- [HasOne](https://godoc.org/gopkg.in/pg.v4#example-DB-Model-HasOne), [HasMany](https://godoc.org/gopkg.in/pg.v4#example-DB-Model-HasMany) and [ManyToMany](https://godoc.org/gopkg.in/pg.v4#example-DB-Model-ManyToMany).
- [Migrations](https://github.com/go-pg/migrations).
- [Sharding](https://github.com/go-pg/sharding).

API docs: http://godoc.org/gopkg.in/pg.v4.
Examples: http://godoc.org/gopkg.in/pg.v4#pkg-examples.

# Table of contents

* [Installation](#installation)
* [Quickstart](#quickstart)
* [Howto](#howto)
* [Writing queries](#writing-queries)
* [Model definition](#model-definition)
* [FAQ](#faq)

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
	Id       int64
	Title    string
	AuthorId int64
	Author   *User
}

func (s Story) String() string {
	return fmt.Sprintf("Story<%d %s %s>", s.Id, s.Title, s.Author)
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
		Title:    "Cool story",
		AuthorId: user1.Id,
	}
	err = db.Create(story1)
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
```

## Howto

Please go through [examples](http://godoc.org/gopkg.in/pg.v4#pkg-examples) to get the idea how to use this package.

## Model definition

Models are defined using Go structs. Order of the struct fields usually does not matter with the only exception being primary key(s) that must defined before any other fields. Otherwise table relationshipts can be recognized incorrectly.

```go
type Genre struct {
	TableName struct{} `sql:"genres"` // specifies custom table name

	Id     int // Id is automatically detected as primary key
	Name   string
	Rating int `sql:"-"` // - is used to ignore field

	Books []Book `pg:",many2many:book_genres"` // many to many relation

	ParentId  int     `sql:",null"`
	Subgenres []Genre `pg:",fk:Parent"` // fk specifies prefix for foreign key (ParentId)
}

func (g Genre) String() string {
	return fmt.Sprintf("Genre<Id=%d Name=%q>", g.Id, g.Name)
}

type Author struct {
	ID    int // both "Id" and "ID" are detected as primary key
	Name  string
	Books []Book // has many relation
}

func (a Author) String() string {
	return fmt.Sprintf("Author<ID=%d Name=%q>", a.ID, a.Name)
}

type BookGenre struct {
	BookId  int `sql:",pk"` // pk tag is used to mark field as primary key
	GenreId int `sql:",pk"`

	Genre_Rating int // belongs to and is copied to Genre model
}

type Book struct {
	Id        int
	Title     string
	AuthorID  int
	Author    *Author // has one relation
	EditorID  int
	Editor    *Author // has one relation
	CreatedAt time.Time

	Genres       []Genre       `pg:",many2many:book_genres" gorm:"many2many:book_genres;"` // many to many relation
	Translations []Translation // has many relation
	Comments     []Comment     `pg:",polymorphic:Trackable"` // has many polymorphic relation
}

func (b Book) String() string {
	return fmt.Sprintf("Book<Id=%d Title=%q>", b.Id, b.Title)
}

type Translation struct {
	Id     int
	BookId int
	Book   *Book // belongs to relation
	Lang   string

	Comments []Comment `pg:",polymorphic:Trackable"` // has many polymorphic relation
}

type Comment struct {
	TrackableId   int    `sql:",pk"` // Book.Id or Translation.Id
	TrackableType string `sql:",pk"` // "book" or "translation"
	Text          string
}
```

## Writing queries

### Select

```go
// Select book by primary key.
err := db.Select(&book)
// SELECT * FROM "books" WHERE id = 1

// Select book title and text.
var title, text string
err := db.Model(&Book{}).Column("title", "text").Where("id = ?", 1).Select(&title, &text)
// SELECT "title", "text" FROM "books" WHERE id = 1

// Select book using WHERE.
err := db.Model(&book).
    Where("id > ?", 100).
    Where("title LIKE ?", "my%").
    Limit(1).
    Select()
// SELECT * FROM "books" WHERE (id > 100) AND (title LIKE 'my%') LIMIT 1

// Select first 20 books.
err := db.Model(&books).Order("id ASC").Limit(20).Select()
// SELECT * FROM "books" ORDER BY id ASC LIMIT 20

// Count books.
count, err := db.Model(&Book{}).Count()
// SELECT COUNT(*) FROM "books"

// Select 20 books and count all books.
count, err := db.Model(&books).Limit(20).SelectAndCount()
// SELECT * FROM "books" LIMIT 20
// SELECT COUNT(*) FROM "books"

// Select 20 books and count estimated number of books.
count, err := db.Model(&books).Limit(20).SelectAndCountEstimate(100000)
// SELECT * FROM "books" LIMIT 20
// EXPLAIN SELECT 2147483647 FROM "books"
// SELECT COUNT(*) FROM "books"

// Select author id and number of books.
var res []struct {
    AuthorId  int
    BookCount int
}
err := db.Model(&Book{}).
    Column("author_id").
    ColumnExpr("count(*) AS book_count").
    Group("author_id").
    Order("book_count DESC").
    Select(&res)
// SELECT "author_id", count(*) AS book_count
// FROM "books" AS "book"
// GROUP BY author_id
// ORDER BY book_count DESC

// Select book ids as PostgreSQL array.
var ids []int
err := db.Model(&Book{}).ColumnExpr("array_agg(id)").Select(pg.Array(&ids))
// SELECT array_agg(id) FROM "books"
```

### Insert

```go
// Insert new book returning primary keys.
err := db.Create(&book)
// INSERT INTO "books" (title, text) VALUES ('my title', 'my text') RETURNING "id"

// Insert new book returning all columns.
err := db.Model(&book).Returning("*").Create()
// INSERT INTO "books" (title, text) VALUES ('my title', 'my text') RETURNING *

// Select existing book by name or create new book.
err := db.Model(&book).
    Where("title = ?title").
    OnConflict("DO NOTHING"). // optional
    SelectOrCreate()
// 1. SELECT * FROM "books" WHERE title = 'my title'
// 2. INSERT INTO "books" (title, text) VALUES ('my title', 'my text') RETURNING "id"
// 3. go to step 1 on error
```

### Update

```go
// Update all columns except primary keys.
err := db.Update(&book)
// UPDATE "books" SET title = 'my title', text = 'my text' WHERE id = 1

// Update only column "title".
res, err := db.Model(&book).Set("title = ?title").Where("id = ?id").Update()
// UPDATE "books" SET title = 'my title' WHERE id = 1

// Update only column "title".
res, err := db.Model(&book).Column("title").Update()
// UPDATE "books" SET title = 'my title' WHERE id = 1

// Update only column "title".
res, err := db.Model(&book).UpdateValues(map[string]interface{}{
    "title": book.Title,
})
// UPDATE "books" SET title = 'my title' WHERE id = 1
```

### Delete

```go
// Delete book by primary key.
err := db.Delete(&book)
// DELETE FROM "books" WHERE id = 1

// Delete book by title.
res, err := db.Model(&book).Where("title = ?title").Delete()
// DELETE FROM "books" WHERE title = 'my title'
```

## FAQ

### Why go-pg

- No `rows.Close` to manually manage connections.
- go-pg automatically maps rows on Go structs and slice.
- go-pg is 2x-10x faster than GORM on querying 100 rows from table.

    ```
    BenchmarkQueryRowsGopgOptimized-4	   10000	    158443 ns/op	   83432 B/op	     625 allocs/op
    BenchmarkQueryRowsGopgReflect-4  	   10000	    189014 ns/op	   94759 B/op	     826 allocs/op
    BenchmarkQueryRowsGopgORM-4      	   10000	    199685 ns/op	   95024 B/op	     830 allocs/op
    BenchmarkQueryRowsStdlibPq-4     	    5000	    242135 ns/op	  161646 B/op	    1324 allocs/op
    BenchmarkQueryRowsGORM-4         	    2000	    704366 ns/op	  396184 B/op	    6767 allocs/op
    ```

- go-pg generates much more effecient queries for joins.

    ##### Has one relation

    ```
    BenchmarkModelHasOneGopg-4                 	    5000	    313091 ns/op	   78481 B/op	    1290 allocs/op
    BenchmarkModelHasOneGORM-4                 	     500	   3849634 ns/op	 1529982 B/op	   71636 allocs/op
    ```

    go-pg:

    ```go
    db.Model(&books).Column("book.*", "Author").Limit(100).Select()
    ```

    ```sql
    SELECT "book".*, "author"."id" AS "author__id", "author"."name" AS "author__name"
    FROM "books" AS "book"
    LEFT JOIN "authors" AS "author" ON "author"."id" = "book"."author_id"
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

    ##### Has many relation

    ```
    BenchmarkModelHasManyGopg-4                	     500	   3034576 ns/op	  409288 B/op	    8700 allocs/op
    BenchmarkModelHasManyGORM-4                	      50	  24677309 ns/op	10121839 B/op	  519411 allocs/op
    ```

    go-pg:

    ```go
    db.Model(&books).Column("book.*", "Translations").Limit(100).Select()
    ```

    ```sql
     SELECT "book".* FROM "books" AS "book" LIMIT 100
     SELECT "translation".* FROM "translations" AS "translation"
     WHERE ("translation"."book_id") IN ((100), (101), ... (199));
    ```

    GORM:

    ```go
    db.Preload("Translations").Limit(100).Find(&books).Error
    ```

    ```sql
    SELECT * FROM "books" LIMIT 100;
    SELECT * FROM "translations"
    WHERE ("book_id" IN (1, 2, ..., 100));
    SELECT * FROM "authors" WHERE ("book_id" IN (1, 2, ..., 100));
    ```

   ##### Many to many relation

    ```
    BenchmarkModelHasMany2ManyGopg-4           	     500	   3329123 ns/op	  394738 B/op	    9739 allocs/op
    BenchmarkModelHasMany2ManyGORM-4           	     200	   7847630 ns/op	 3350304 B/op	   66239 allocs/op
    ```

    go-pg:

    ```go
    db.Model(&books).Column("book.*", "Genres").Limit(100).Select()
    ```

    ```sql
    SELECT "book"."id" FROM "books" AS "book" LIMIT 100;
    SELECT * FROM "genres" AS "genre"
    JOIN "book_genres" AS "book_genre" ON ("book_genre"."book_id") IN ((1), (2), ..., (100))
    WHERE "genre"."id" = "book_genre"."genre_id";
    ```

    GORM:

    ```go
    db.Preload("Genres").Limit(100).Find(&books).Error
    ```

    ```sql
    SELECT * FROM "books" LIMIT 100;
    SELECT * FROM "genres"
    INNER JOIN "book_genres" ON "book_genres"."genre_id" = "genres"."id"
    WHERE ("book_genres"."book_id" IN ((1), (2), ..., (100)));
    ```

### How can I view queries this library generates?

You can configure PostgreSQL to log every query by adding following lines to your postgresql.conf (usually /etc/postgresql/9.5/main/postgresql.conf):

```
log_statement = 'all'
log_min_duration_statement = 0
```

Then just tail the log file:

```shell
tail -f /var/log/postgresql/postgresql-9.5-main.log
```
