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
	// title 1
	// title 2
	// title 3
}

func ExampleDB_Model_first_and_last() {
	var book Book
	err := db.Model(&book).Last()
	Expect(err).NotTo(HaveOccurred())
	Expect(book.Id).To(Equal(102))
	Expect(book.CreatedAt.IsZero()).To(BeFalse())
}

func ExampleDB_Model_select_all() {
	var book Book
	err := db.Model(&book).Columns("book.*").First()
	Expect(err).NotTo(HaveOccurred())
	Expect(book.Id).To(Equal(100))
	Expect(book.Title).To(Equal("book 1"))
}

func ExampleDB_Model_select_columns() {
	var book Book
	err := db.Model(&book).
		Columns("book.id").
		First()
	Expect(err).NotTo(HaveOccurred())
	Expect(book.Id).To(Equal(100))
	Expect(book.Title).To(BeZero())
	Expect(book.Author.ID).To(Equal(10))
	Expect(book.Author.Name).To(Equal("author 1"))
}

func ExampleDB_Model_count() {
	var count int
	err := db.Model(&Book{}).Count(&count)
	Expect(err).NotTo(HaveOccurred())
	Expect(count).To(Equal(1))
}

func ExampleDB_Update() {
	err := db.Update(&Book{
		Id:    100,
		Title: "updated book 1",
	})
	Expect(err).NotTo(HaveOccurred())

	var book Book
	err = db.Model(&book).Where("id = ?", 100).First()
	Expect(err).NotTo(HaveOccurred())
	Expect(book.Title).To(Equal("updated book 1"))
	Expect(book.AuthorID).To(Equal(0))
}

func ExampleDB_Model_update_column() {
	book := &Book{
		Title:    "title 1",
		AuthorID: 1,
	}
	err := db.Create(book)
	if err != nil {
		panic(err)
	}

	book.Title = "title 2"
	book.AuthorID = 2

	err = db.Model(book).Columns("title").Returning("*").Update()
	if err != nil {
		panic(err)
	}

	fmt.Printf("title=%q author_id=%d\n", book.Title, book.AuthorID)
	// Output: title="title 2" author_id=1
}

func ExampleDB_Model_update_function() {
	id := 100
	data := map[string]interface{}{
		"title": pg.Q("concat(?, title, ?)", "prefix ", " suffix"),
	}

	var book Book
	err := db.Model(&book).
		Where("id = ?", id).
		Returning("*").
		UpdateValues(data)
	Expect(err).NotTo(HaveOccurred())
	Expect(book.Id).To(Equal(id))
	Expect(book.Title).To(Equal("prefix book 1 suffix"))
}

func ExampleDB_Model_update_multi_models() {
	ids := pg.Ints{100, 101}
	data := map[string]interface{}{
		"title": pg.Q("concat(?, title, ?)", "prefix ", " suffix"),
	}

	err := db.Model(&Book{}).
		Where("id IN (?)", ids).
		UpdateValues(data)
	Expect(err).NotTo(HaveOccurred())

	var books []Book
	err = db.Model(&books).
		Where("id IN (?)", ids).
		Order("id ASC").
		Select()
	Expect(err).NotTo(HaveOccurred())
	Expect(books).To(HaveLen(2))
	Expect(books[0].Title).To(Equal("prefix book 1 suffix"))
	Expect(books[1].Title).To(Equal("prefix book 2 suffix"))
}

func ExampleDB_Delete() {
	err := db.Delete(&Book{Id: 100})
	Expect(err).NotTo(HaveOccurred())

	err = db.Model(&Book{}).Where("id = ?", 100).First()
	Expect(err).To(Equal(pg.ErrNoRows))
}

func ExampleDB_Delete_multi_models() {
	ids := pg.Ints{100, 101}

	err := db.Model(&Book{}).Where("id IN (?)", ids).Delete()
	Expect(err).NotTo(HaveOccurred())

	var count int
	err = db.Model(&Book{}).Count(&count)
	Expect(err).NotTo(HaveOccurred())
	Expect(count).To(Equal(1))
}
