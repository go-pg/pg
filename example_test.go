package pg_test

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	"gopkg.in/pg.v4"
	"gopkg.in/pg.v4/orm"
)

var db *pg.DB

func init() {
	db = pg.Connect(&pg.Options{
		User: "postgres",
	})
	if err := createTestSchema(db); err != nil {
		panic(err)
	}
}

func connectDB() *pg.DB {
	db := pg.Connect(&pg.Options{
		User: "postgres",
	})
	if err := createTestSchema(db); err != nil {
		panic(err)
	}
	return db
}

func ExampleConnect() {
	db := pg.Connect(&pg.Options{
		User: "postgres",
	})
	err := db.Close()
	fmt.Println(err)
	// Output: <nil>
}

func ExampleDB_QueryOne() {
	var user struct {
		Name string
	}

	res, err := db.QueryOne(&user, `
        WITH users (name) AS (VALUES (?))
        SELECT * FROM users
    `, "admin")
	if err != nil {
		panic(err)
	}
	fmt.Println(res.Affected())
	fmt.Println(user)
	// Output: 1
	// {admin}
}

func ExampleDB_QueryOne_returning_id() {
	_, err := db.Exec(`CREATE TEMP TABLE users(id serial, name varchar(500))`)
	if err != nil {
		panic(err)
	}

	var user struct {
		Id   int32
		Name string
	}
	user.Name = "admin"

	_, err = db.QueryOne(&user, `
        INSERT INTO users (name) VALUES (?name) RETURNING id
    `, user)
	if err != nil {
		panic(err)
	}
	fmt.Println(user)
	// Output: {1 admin}
}

func ExampleDB_QueryOne_Scan() {
	var s1, s2 string
	_, err := db.QueryOne(pg.Scan(&s1, &s2), `SELECT ?, ?`, "foo", "bar")
	fmt.Println(s1, s2, err)
	// Output: foo bar <nil>
}

func ExampleDB_Exec() {
	res, err := db.Exec(`CREATE TEMP TABLE test()`)
	fmt.Println(res.Affected(), err)
	// Output: -1 <nil>
}

func ExampleListener() {
	ln, err := db.Listen("mychan")
	if err != nil {
		panic(err)
	}

	wait := make(chan struct{}, 2)
	go func() {
		wait <- struct{}{}
		channel, payload, err := ln.Receive()
		fmt.Printf("%s %q %v", channel, payload, err)
		wait <- struct{}{}
	}()

	<-wait
	db.Exec("NOTIFY mychan, ?", "hello world")
	<-wait

	// Output: mychan "hello world" <nil>
}

func ExampleDB_Begin() {
	tx, err := db.Begin()
	if err != nil {
		panic(err)
	}

	_, err = tx.Exec(`CREATE TABLE tx_test()`)
	if err != nil {
		panic(err)
	}

	err = tx.Rollback()
	if err != nil {
		panic(err)
	}

	_, err = db.Exec(`SELECT * FROM tx_test`)
	fmt.Println(err)
	// Output: ERROR #42P01 relation "tx_test" does not exist:
}

func ExampleDB_Prepare() {
	stmt, err := db.Prepare(`SELECT $1::text, $2::text`)
	if err != nil {
		panic(err)
	}

	var s1, s2 string
	_, err = stmt.QueryOne(pg.Scan(&s1, &s2), "foo", "bar")
	fmt.Println(s1, s2, err)
	// Output: foo bar <nil>
}

func ExampleInts() {
	var nums pg.Ints
	_, err := db.Query(&nums, `SELECT generate_series(0, 10)`)
	fmt.Println(nums, err)
	// Output: [0 1 2 3 4 5 6 7 8 9 10] <nil>
}

func ExampleInts_in() {
	ids := pg.Ints{1, 2, 3}
	q, err := orm.FormatQuery(`SELECT * FROM table WHERE id IN (?)`, ids)
	fmt.Println(q, err)
	// Output: SELECT * FROM table WHERE id IN (1,2,3) <nil>
}

func ExampleStrings() {
	var strs pg.Strings
	_, err := db.Query(
		&strs, `WITH users AS (VALUES ('foo'), ('bar')) SELECT * FROM users`)
	fmt.Println(strs, err)
	// Output: [foo bar] <nil>
}

func ExampleDB_CopyFrom() {
	_, err := db.Exec(`CREATE TEMP TABLE words(word text, len int)`)
	if err != nil {
		panic(err)
	}

	r := strings.NewReader("hello,5\nfoo,3\n")
	_, err = db.CopyFrom(r, `COPY words FROM STDIN WITH CSV`)
	if err != nil {
		panic(err)
	}

	buf := &bytes.Buffer{}
	_, err = db.CopyTo(&NopWriteCloser{buf}, `COPY words TO STDOUT WITH CSV`)
	if err != nil {
		panic(err)
	}
	fmt.Println(buf.String())
	// Output: hello,5
	// foo,3
}

func ExampleDB_WithTimeout() {
	var count int
	// Use bigger timeout since this query is known to be slow.
	_, err := db.WithTimeout(time.Minute).QueryOne(pg.Scan(&count), `
		SELECT count(*) FROM big_table
	`)
	if err != nil {
		panic(err)
	}
}

func ExampleDB_Create() {
	book1 := Book{
		Title:    "title 1",
		AuthorID: 1,
	}
	book2 := Book{
		Title:    "title 2",
		AuthorID: 2,
	}
	book3 := Book{
		Title:    "title 3",
		AuthorID: 2,
	}

	db := connectDB()
	err := db.Create(&book1)
	if err != nil {
		panic(err)
	}

	err = db.Create(&book2)
	if err != nil {
		panic(err)
	}

	err = db.Create(&book3)
	if err != nil {
		panic(err)
	}
}

func ExampleDB_Model_first_and_last() {
	ExampleDB_Create()

	var firstBook Book
	err := db.Model(&firstBook).First()
	if err != nil {
		panic(err)
	}

	var lastBook Book
	err = db.Model(&lastBook).Last()
	if err != nil {
		panic(err)
	}

	fmt.Printf("%d, %d", firstBook.Id, lastBook.Id)
	// Output: 1, 3
}

func ExampleDB_Model_select_all() {
	db := connectDB()
	book := Book{
		Title:    "title 1",
		AuthorID: 2,
	}

	err := db.Create(&book)
	if err != nil {
		panic(err)
	}

	err = db.Model(&book).Columns("book.*").First()
	if err != nil {
		panic(err)
	}
	fmt.Printf("%d %q %d", book.Id, book.Title, book.AuthorID)
	// Output: 1 "title 1" 2
}

func ExampleDB_Model_select_columns() {
	db := connectDB()

	author := Author{
		ID:   1,
		Name: "author 1",
	}
	err := db.Create(&author)
	if err != nil {
		panic(err)
	}

	book := Book{
		Title:    "title 1",
		AuthorID: 1,
	}
	err = db.Create(&book)
	if err != nil {
		panic(err)
	}

	err = db.Model(&book).
		Columns("book.id").
		Where("id = ?", 1).
		First()
	if err != nil {
		panic(err)
	}

	fmt.Printf("%d, %q",
		book.Id, book.Title,
	)
	// Output: 1, "title 1"
}

func ExampleDB_Model_count() {
	ExampleDB_Create()
	var count int
	err := db.Model(Book{}).Count(&count)
	if err != nil {
		panic(err)
	}

	fmt.Printf("%d", count)
	// Output: 3
}

func ExampleDB_Update() {
	db := connectDB()
	book := Book{
		Title:    "title 1",
		AuthorID: 1,
	}

	err := db.Create(&book)
	if err != nil {
		panic(err)
	}

	err = db.Update(&Book{
		Id:    1,
		Title: "updated book 1",
	})
	if err != nil {
		panic(err)
	}

	err = db.Model(&book).Where("id = ?", 1).First()
	if err != nil {
		panic(err)
	}

	fmt.Printf("%q %d", book.Title, book.AuthorID)
	// Output: "updated book 1" 0
}

func ExampleDB_Model_update_column() {
	db := connectDB()

	book := Book{
		Title:    "title 1",
		AuthorID: 1,
	}
	err := db.Create(&book)
	if err != nil {
		panic(err)
	}

	book.Title = "title 2"
	book.AuthorID = 2

	err = db.Model(&book).Columns("title").Returning("*").Update()
	if err != nil {
		panic(err)
	}

	fmt.Printf("%q %d\n", book.Title, book.AuthorID)
	// Output: "title 2" 1
}

func ExampleDB_Model_update_function() {
	db := connectDB()

	book := Book{
		Title:    "title 1",
		AuthorID: 1,
	}
	err := db.Create(&book)
	if err != nil {
		panic(err)
	}

	id := 1
	data := map[string]interface{}{
		"title": pg.Q("concat(?, title, ?)", "prefix ", " suffix"),
	}

	err = db.Model(&book).
		Where("id = ?", id).
		Returning("*").
		UpdateValues(data)

	if err != nil {
		panic(err)
	}

	fmt.Printf("%d %q", book.Id, book.Title)
	// Output: 1 "prefix title 1 suffix"
}

func ExampleDB_Model_update_multi_models() {
	ExampleDB_Create()
	ids := pg.Ints{1, 2}
	data := map[string]interface{}{
		"title": pg.Q("concat(?, title, ?)", "prefix ", " suffix"),
	}

	err := db.Model(&Book{}).
		Where("id IN (?)", ids).
		UpdateValues(data)
	if err != nil {
		panic(err)
	}

	var books []Book
	err = db.Model(&books).
		Where("id IN (?)", ids).
		Order("id ASC").
		Select()
	if err != nil {
		panic(err)
	}

	fmt.Printf("%q %q\n", books[0].Title, books[1].Title)
	fmt.Printf("total of books: %d", len(books))
	// Output: "prefix title 1 suffix" "prefix title 2 suffix"
	// total of books: 2
}

func ExampleDB_Delete() {
	db := connectDB()

	book := Book{
		Title:    "title 1",
		AuthorID: 1,
	}
	err := db.Create(&book)
	if err != nil {
		panic(err)
	}

	err = db.Delete(&Book{Id: 1})
	if err != nil {
		panic(err)
	}

	err = db.Model(&Book{}).Where("id = ?", 1).First()
	if err != pg.ErrNoRows {
		panic(err)
	}
}

func ExampleDB_Delete_multi_models() {
	ExampleDB_Create()
	ids := pg.Ints{1, 2, 3}

	err := db.Model(&Book{}).Where("id IN (?)", ids).Delete()
	if err != nil {
		panic(err)
	}

	var count int
	err = db.Model(&Book{}).Count(&count)
	if err != nil {
		panic(err)
	}

	fmt.Printf("count: %d", count)
	// Output: count: 0
}
