# PostgreSQL client for Golang [![Build Status](https://travis-ci.org/go-pg/pg.svg)](https://travis-ci.org/go-pg/pg)

Supports:

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

API docs: http://godoc.org/gopkg.in/pg.v5.
Examples: http://godoc.org/gopkg.in/pg.v5#pkg-examples.

# Table of contents

* [Installation](#installation)
* [Quickstart](#quickstart)
* [Model definition](#model-definition)
* [Model hooks](#model-hooks)
* [Writing queries](#writing-queries)
  * [Placeholders](#placeholders)
  * [Select](#select)
  * [Column names](#column-names)
  * [Reusing queries](#reusing-queries)
  * [Insert](#insert)
  * [Update](#update)
  * [Delete](#delete)
  * [Has one](#has-one)
  * [Belongs to](#belongs-to)
  * [Has many](#has-many)
  * [Has many to many](#has-many-to-many)
* [Howto](#howto)
* [FAQ](#faq)

## Installation

Install:

    go get gopkg.in/pg.v5

## Quickstart

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
```

## Model definition

Models are defined using Go structs which are mapped to PostgreSQL tables. Exported struct fields are mapped to table columns. Table name and alias are automatically derived from struct name by underscoring it; table name is also pluralized (struct `Genre` -> table `genres AS genre`). Default table name can be overrided using `tableName` field. Column name is derived from struct field name by underscoring it (field `ParentId` -> column `parent_id`). Default column name can be overrided using `sql` tag. Order of struct fields does not matter with the only exception being primary keys that must be defined before any other fields. Otherwise table relationships can be recognized incorrectly.

Please *note* that most struct tags in the following example have the same values as the defaults and are included only for demonstration purposes.

```go
type Genre struct {
	// tableName is an optional field that specifies custom table name and alias.
	// By default go-pg generates table name and alias from struct name.
	tableName struct{} `sql:"genres,alias:genre"` // default values are the same

	Id     int // Id is automatically detected as primary key
	Name   string
	Rating int `sql:"-"` // - is used to ignore field

	Books []Book `pg:",many2many:book_genres"` // many to many relation

	ParentId  int
	Subgenres []Genre `pg:",fk:Parent"` // fk specifies prefix for foreign key (ParentId)
}

func (g Genre) String() string {
	return fmt.Sprintf("Genre<Id=%d Name=%q>", g.Id, g.Name)
}

type Author struct {
	ID    int // both "Id" and "ID" are detected as primary key
	Name  string
	Books []*Book // has many relation
}

func (a Author) String() string {
	return fmt.Sprintf("Author<ID=%d Name=%q>", a.ID, a.Name)
}

type BookGenre struct {
	tableName struct{} `sql:",alias:bg"` // custom table alias

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

func (b *Book) BeforeInsert(db orm.DB) error {
	if b.CreatedAt.IsZero() {
		b.CreatedAt = time.Now()
	}
	return nil
}

// BookWithCommentCount is like Book model, but has additional CommentCount
// field that is used to select data into it. The use of `pg:",override"` tag
// is essential here and it overrides internal model properties such as table name.
type BookWithCommentCount struct {
	Book `pg:",override"`

	CommentCount int
}

type Translation struct {
	tableName struct{} `sql:",alias:tr"` // custom table alias

	Id     int
	BookId int
	Book   *Book // belongs to relation
	Lang   string

	Comments []Comment `pg:",polymorphic:Trackable"` // has many polymorphic relation
}

type Comment struct {
	TrackableId   int    // Book.Id or Translation.Id
	TrackableType string // "Book" or "Translation"
	Text          string
}
```

## Model hooks

Models support optional hooks that accept `orm.DB` interface which value can be either `*pg.DB` or `*pg.Tx`.

```go
// AfterQuery is called after the model is loaded from database.
func (b *Book) AfterQuery(db orm.DB) error {
    return updateBookCache(b)
}

// AfterSelect is called after the model and all its relations (e.g. has one)
// are loaded from database.
func (b *Book) AfterSelect(db orm.DB) error {
    return updateBookCache(b)
}

func (b *Book) BeforeInsert(db orm.DB) error {
    if b.CreatedAt.IsZero() {
        b.CreatedAt = time.Now()
    }
    return nil
}

func (b *Book) AfterInsert(db orm.DB) error {
    return updateBookCache(b)
}

func (b *Book) BeforeUpdate(db orm.DB) error {
   return nil
}

func (b *Book) AfterUpdate(db orm.DB) error {
   return nil
}

func (b *Book) BeforeDelete(db orm.DB) error {
   return nil
}

func (b *Book) AfterDelete(db orm.DB) error {
   return nil
}
```

## Writing queries

### Placeholders

```go
package pg_test

import (
	"fmt"

	"gopkg.in/pg.v5"
)

type Params struct {
	X int
	Y int
}

func (p *Params) Sum() int {
	return p.X + p.Y
}

// go-pg recognizes placeholders (`?`) in queries and replaces them
// with parameters when queries are executed. Parameters are escaped
// before replacing according to PostgreSQL rules. Specifically:
//   - all parameters are properly quoted against SQL injections;
//   - null byte is removed;
//   - JSON/JSONB gets `\u0000` escaped as `\\u0000`.
func Example_placeholders() {
	var num int

	// Simple params.
	_, err := db.Query(pg.Scan(&num), "SELECT ?", 42)
	if err != nil {
		panic(err)
	}
	fmt.Println("simple:", num)

	// Indexed params.
	_, err = db.Query(pg.Scan(&num), "SELECT ?0 + ?0", 1)
	if err != nil {
		panic(err)
	}
	fmt.Println("indexed:", num)

	// Named params.
	params := &Params{
		X: 1,
		Y: 1,
	}
	_, err = db.Query(pg.Scan(&num), "SELECT ?x + ?y + ?Sum", &params)
	if err != nil {
		panic(err)
	}
	fmt.Println("named:", num)

	// Global params.
	_, err = db.WithParam("z", 1).Query(pg.Scan(&num), "SELECT ?x + ?y + ?z", &params)
	if err != nil {
		panic(err)
	}
	fmt.Println("global:", num)

	// Output: simple: 42
	// indexed: 2
	// named: 4
	// global: 3
}
```

### Select

```go
// Select book by primary key.
err := db.Select(&book)
// SELECT "book"."id", "book"."title", "book"."text"
// FROM "books" WHERE id = 1

// Select only book title and text.
err := db.Model(&book).Column("title", "text").Where("id = ?", 1).Select()
// SELECT "title", "text"
// FROM "books" WHERE id = 1

// Select only book title and text into variables.
var title, text string
err := db.Model(&Book{}).Column("title", "text").Where("id = ?", 1).Select(&title, &text)
// SELECT "title", "text"
// FROM "books" WHERE id = 1

// Select book using WHERE ... AND ...
err := db.Model(&book).
    Where("id > ?", 100).
    Where("title LIKE ?", "my%").
    Limit(1).
    Select()
// SELECT "book"."id", "book"."title", "book"."text"
// FROM "books"
// WHERE (id > 100) AND (title LIKE 'my%')
// LIMIT 1

// Select book using WHERE ... OR ...
err := db.Model(&book).
    Where("id > ?", 100).
    WhereOr("title LIKE ?", "my%").
    Limit(1).
    Select()
// SELECT "book"."id", "book"."title", "book"."text"
// FROM "books"
// WHERE (id > 100) OR (title LIKE 'my%')
// LIMIT 1

// Select first 20 books.
err := db.Model(&books).Order("id ASC").Limit(20).Select()
// SELECT "book"."id", "book"."title", "book"."text"
// FROM "books"
// ORDER BY id ASC LIMIT 20

// Count books.
count, err := db.Model(&Book{}).Count()
// SELECT count(*) FROM "books"

// Select 20 books and count all books.
count, err := db.Model(&books).Limit(20).SelectAndCount()
// SELECT "book"."id", "book"."title", "book"."text"
// FROM "books" LIMIT 20
//
// SELECT count(*) FROM "books"

// Select 20 books and count estimated number of books.
count, err := db.Model(&books).Limit(20).SelectAndCountEstimate(100000)
// SELECT "book"."id", "book"."title", "book"."text"
// FROM "books" LIMIT 20
//
// EXPLAIN SELECT 2147483647 FROM "books"
// SELECT count(*) FROM "books"

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

// Select books using WITH statement.
authorBooks := db.Model(&Book{}).Where("author_id = ?", 1)
err := db.Model(nil).
    With("author_books", authorBooks).
    Table("author_books").
    Select(&books)
// WITH "author_books" AS (
//   SELECT "book"."id", "book"."title", "book"."text"
//   FROM "books" AS "book" WHERE (author_id = 1)
// )
// SELECT * FROM "author_books"

// Same query using WrapWith.
err := db.Model(&books).
    Where("author_id = ?", 1).
    WrapWith("author_books").
    Table("author_books").
    Select(&books)
// WITH "author_books" AS (
//   SELECT "book"."id", "book"."title", "book"."text"
//   FROM "books" AS "book" WHERE (author_id = 1)
// )
// SELECT * FROM "author_books"
```

### Column names

```go
// Select book and associated author.
err := db.Model(&book).Column("Author").Select()
// SELECT
//   "book"."id", "book"."title", "book"."text",
//   "author"."id" AS "author__id", "author"."name" AS "author__name"
// FROM "books"
// LEFT JOIN "users" AS "author" ON "author"."id" = "book"."author_id"
// WHERE id = 1

// Select book id and associated author id.
err := db.Model(&book).Column("book.id", "Author.id").Select()
// SELECT "book"."id", "author"."id" AS "author__id"
// FROM "books"
// LEFT JOIN "users" AS "author" ON "author"."id" = "book"."author_id"
// WHERE id = 1

// Select book and join author without selecting it.
err := db.Model(&book).Column("Author._").Select()
// SELECT "book"."id"
// FROM "books"
// LEFT JOIN "users" AS "author" ON "author"."id" = "book"."author_id"
// WHERE id = 1

// Join and select book author without selecting book.
err := db.Model(&book).Column("_", "Author").Select()
// SELECT "author"."id" AS "author__id", "author"."name" AS "author__name"
// FROM "books"
// LEFT JOIN "users" AS "author" ON "author"."id" = "book"."author_id"
// WHERE id = 1
```

### Reusing queries

```go
// Pager sets LIMIT and OFFSET from the URL values:
//   - ?limit=10 - sets q.Limit(10), max limit is 1000.
//   - ?page=5 - sets q.Offset((page - 1) * limit), max offset is 1000000.
func Pager(urlValues url.Values, defaultLimit int) func(*Query) (*Query, error) {
	return func(q *Query) (*Query, error) {
		const maxLimit = 1000
		const maxOffset = 1e6

		limit, err := intParam(urlValues, "limit")
		if err != nil {
			return nil, err
		}
		if limit < 1 {
			limit = defaultLimit
		} else if limit > maxLimit {
			return nil, fmt.Errorf("limit can't bigger than %d", maxLimit)
		}
		if limit > 0 {
			q = q.Limit(limit)
		}

		page, err := intParam(urlValues, "page")
		if err != nil {
			return nil, err
		}
		if page > 0 {
			offset := (page - 1) * limit
			if offset > maxOffset {
				return nil, fmt.Errorf("offset can't bigger than %d", maxOffset)
			}
			q = q.Offset(offset)
		}

		return q, nil
	}
}

var books []Book
err := db.Model(&books).Apply(orm.Pager(req.URL.Query())).Select()
// SELECT * FROM "books" LIMIT 20
```

### Insert

```go
// Insert new book returning primary keys.
err := db.Insert(&book)
// INSERT INTO "books" (title, text) VALUES ('my title', 'my text') RETURNING "id"

// Insert new book returning all columns.
err := db.Model(&book).Returning("*").Insert()
// INSERT INTO "books" (title, text) VALUES ('my title', 'my text') RETURNING *

// Select existing book by name or create new book.
err := db.Model(&book).
    Where("title = ?title").
    OnConflict("DO NOTHING"). // optional
    SelectOrInsert()
// 1. SELECT * FROM "books" WHERE title = 'my title'
// 2. INSERT INTO "books" (title, text) VALUES ('my title', 'my text') RETURNING "id"
// 3. go to step 1 on error

// Insert new book or update existing one.
_, err := db.Model(&book).
    OnConflict("(id) DO UPDATE").
    Set("title = ?title").
    Insert()
// INSERT INTO "books" ("id", "title") VALUES (100, 'my title')
// ON CONFLICT (id) DO UPDATE SET title = 'title version #1'
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

// Upper column "title" and scan it.
var title string
res, err := db.Model(&book).
    Set("title = upper(title)").
    Where("id = ?", 1).
    Returning("title").
    Update(&title)
// UPDATE "books" SET title = upper(title) WHERE id = 1 RETURNING title
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

### Has one

Following examples selects users joining their profiles:

```go
type Profile struct {
    Id   int
    Lang string
}

// User has one profile.
type User struct {
    Id        int
    Name      string
    ProfileId int
    Profile   *Profile
}

db := connect()
defer db.Close()

qs := []string{
    "CREATE TEMP TABLE users (id int, name text, profile_id int)",
    "CREATE TEMP TABLE profiles (id int, lang text)",
    "INSERT INTO users VALUES (1, 'user 1', 1), (2, 'user 2', 2)",
    "INSERT INTO profiles VALUES (1, 'en'), (2, 'ru')",
}
for _, q := range qs {
    _, err := db.Exec(q)
    if err != nil {
        panic(err)
    }
}

// Select users joining their profiles with following query:
//
// SELECT
//   "user".*,
//   "profile"."id" AS "profile__id",
//   "profile"."lang" AS "profile__lang"
// FROM "users" AS "user"
// LEFT JOIN "profiles" AS "profile" ON "profile"."id" = "user"."profile_id"

var users []User
err := db.Model(&users).
    Column("user.*", "Profile").
    Select()
if err != nil {
    panic(err)
}

fmt.Println(len(users), "results")
fmt.Println(users[0].Id, users[0].Name, users[0].Profile)
fmt.Println(users[1].Id, users[1].Name, users[1].Profile)
// Output: 2 results
// 1 user 1 &{1 en}
// 2 user 2 &{2 ru}
```

### Belongs to

Following examples selects users joining their profiles:

```go
// Profile belongs to User.
type Profile struct {
    Id     int
    Lang   string
    UserId int
}

type User struct {
    Id      int
    Name    string
    Profile *Profile
}

db := connect()
defer db.Close()

qs := []string{
    "CREATE TEMP TABLE users (id int, name text)",
    "CREATE TEMP TABLE profiles (id int, lang text, user_id int)",
    "INSERT INTO users VALUES (1, 'user 1'), (2, 'user 2')",
    "INSERT INTO profiles VALUES (1, 'en', 1), (2, 'ru', 2)",
}
for _, q := range qs {
    _, err := db.Exec(q)
    if err != nil {
        panic(err)
    }
}

// Select users joining their profiles with following query:
//
// SELECT
//   "user".*,
//   "profile"."id" AS "profile__id",
//   "profile"."lang" AS "profile__lang",
//   "profile"."user_id" AS "profile__user_id"
// FROM "users" AS "user"
// LEFT JOIN "profiles" AS "profile" ON "profile"."user_id" = "user"."id"

var users []User
err := db.Model(&users).
    Column("user.*", "Profile").
    Select()
if err != nil {
    panic(err)
}

fmt.Println(len(users), "results")
fmt.Println(users[0].Id, users[0].Name, users[0].Profile)
fmt.Println(users[1].Id, users[1].Name, users[1].Profile)
// Output: 2 results
// 1 user 1 &{1 en 1}
// 2 user 2 &{2 ru 2}
```

### Has many

Following example selects first user and all his active profiles:

```go
type Profile struct {
    Id     int
    Lang   string
    Active bool
    UserId int
}

// User has many profiles.
type User struct {
    Id       int
    Name     string
    Profiles []*Profile
}

db := connect()
defer db.Close()

qs := []string{
    "CREATE TEMP TABLE users (id int, name text)",
    "CREATE TEMP TABLE profiles (id int, lang text, active bool, user_id int)",
    "INSERT INTO users VALUES (1, 'user 1')",
    "INSERT INTO profiles VALUES (1, 'en', TRUE, 1), (2, 'ru', TRUE, 1), (3, 'md', FALSE, 1)",
}
for _, q := range qs {
    _, err := db.Exec(q)
    if err != nil {
        panic(err)
    }
}

// Select user and all his active profiles using following queries:
//
// SELECT "user".* FROM "users" AS "user" ORDER BY "user"."id" LIMIT 1
//
// SELECT "profile".* FROM "profiles" AS "profile"
// WHERE (active IS TRUE) AND (("profile"."user_id") IN ((1)))

var user User
err := db.Model(&user).
    Column("user.*", "Profiles").
    Relation("Profiles", func(q *orm.Query) *orm.Query {
        return q.Where("active IS TRUE")
    }).
    First()
if err != nil {
    panic(err)
}
fmt.Println(user.Id, user.Name, user.Profiles[0], user.Profiles[1])
// Output: 1 user 1 &{1 en true 1} &{2 ru true 1}
```

### Has many to many

Following example selects one item and all subitems using itermediary `item_to_items` table.

```go
type Item struct {
    Id    int
    Items []Item `pg:",many2many:item_to_items,joinFK:Sub"`
}

err := db.Model(&item).Column("item.*", "Items").First()
// SELECT "item".* FROM "items" AS "item" ORDER BY "item"."id" LIMIT 1
//
// SELECT * FROM "items" AS "item"
// JOIN "item_to_items" ON ("item_to_items"."item_id") IN ((1))
// WHERE ("item"."id" = "item_to_items"."sub_id")
```

## Howto

Please go through [examples](http://godoc.org/gopkg.in/pg.v5#pkg-examples) to get the idea how to use this package.

## FAQ

### Why go-pg

- No `rows.Close` to manually manage connections.
- go-pg automatically maps rows on Go structs and slice.
- go-pg is 2x-10x faster than GORM on querying 100 rows from table.

    ```
    BenchmarkQueryRowsGopgOptimized-4          	   10000	    122138 ns/op	   83472 B/op	     625 allocs/op
    BenchmarkQueryRowsGopgReflect-4            	   10000	    137208 ns/op	   87488 B/op	     736 allocs/op
    BenchmarkQueryRowsGopgORM-4                	   10000	    142029 ns/op	   87920 B/op	     741 allocs/op
    BenchmarkQueryRowsStdlibPq-4               	   10000	    162664 ns/op	  161631 B/op	    1324 allocs/op
    BenchmarkQueryRowsGORM-4                   	    2000	    569147 ns/op	  415272 B/op	    6966 allocs/op
    ```

- go-pg generates much more effecient queries for joins.

    ##### Has one relation

    ```
    BenchmarkModelHasOneGopg-4                 	    5000	    273181 ns/op	   62692 B/op	    1080 allocs/op
    BenchmarkModelHasOneGORM-4                 	     500	   3320562 ns/op	 1528489 B/op	   71630 allocs/op
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
    BenchmarkModelHasManyGopg-4                	     500	   2825899 ns/op	  311556 B/op	    5501 allocs/op
    BenchmarkModelHasManyGORM-4                	     200	   7538012 ns/op	 3333023 B/op	   71756 allocs/op
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
    BenchmarkModelHasMany2ManyGopg-4           	     500	   3184262 ns/op	  397883 B/op	    7523 allocs/op
    BenchmarkModelHasMany2ManyGORM-4           	     200	   8233222 ns/op	 3686341 B/op	   77236 allocs/op
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

You can setup query logger like this:

```go
pg.SetQueryLogger(log.New(os.Stdout, "", log.LstdFlags))
```

Or you can configure PostgreSQL to log every query by adding following lines to your postgresql.conf (usually /etc/postgresql/9.5/main/postgresql.conf):

```
log_statement = 'all'
log_min_duration_statement = 0
```

Then just tail the log file:

```shell
tail -f /var/log/postgresql/postgresql-9.5-main.log
```
