package pg_test

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/vmihailenco/pg"
)

var db *pg.DB

func init() {
	db = pg.Connect(&pg.Options{
		User: "postgres",
	})
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
	fmt.Println(res.Affected(), err)
	fmt.Println(user)
	// Output: 1 <nil>
	// {admin}
}

func ExampleDB_Exec() {
	res, err := db.Exec("CREATE TEMP TABLE test()")
	fmt.Println(res.Affected(), err)
	// Output: 0 <nil>
}

func ExampleLoadInto() {
	var s1, s2 string
	_, err := db.QueryOne(pg.LoadInto(&s1, &s2), "SELECT ?, ?", "foo", "bar")
	fmt.Println(s1, s2, err)
	// Output: foo bar <nil>
}

func ExampleListener() {
	ln, _ := db.Listen("mychan")

	done := make(chan struct{})
	go func() {
		channel, payload, err := ln.Receive()
		fmt.Printf("%s %q %v", channel, payload, err)
		done <- struct{}{}
	}()

	_, _ = db.Exec("NOTIFY mychan, ?", "hello world")
	<-done

	// Output: mychan "hello world" <nil>
}

func ExampleDB_Begin() {
	tx, err := db.Begin()
	if err != nil {
		panic(err)
	}

	_, err = tx.Exec("CREATE TEMP TABLE test()")
	if err != nil {
		panic(err)
	}

	err = tx.Rollback()
	if err != nil {
		panic(err)
	}

	_, err = db.Exec("SELECT * FROM test")
	fmt.Println(err)
	// Output: ERROR #42P01 relation "test" does not exist:
}

func ExampleDB_Prepare() {
	stmt, err := db.Prepare("SELECT $1::text, $2::text")
	if err != nil {
		panic(err)
	}

	var s1, s2 string
	_, err = stmt.QueryOne(pg.LoadInto(&s1, &s2), "foo", "bar")
	fmt.Println(s1, s2, err)
	// Output: foo bar <nil>
}

func ExampleInts() {
	var nums pg.Ints
	_, err := db.Query(&nums, "SELECT generate_series(0, 10)")
	fmt.Println(nums, err)
	// Output: [0 1 2 3 4 5 6 7 8 9 10] <nil>
}

func ExampleStrings() {
	var strs pg.Strings
	_, err := db.Query(
		&strs, "WITH users AS (VALUES ('foo'), ('bar')) SELECT * FROM users")
	fmt.Println(strs, err)
	// Output: [foo bar] <nil>
}

func ExampleDB_CopyFrom() {
	_, err := db.Exec("CREATE TEMP TABLE words(word text, len int)")
	if err != nil {
		panic(err)
	}

	r := strings.NewReader("hello,5\nfoo,3\n")
	_, err = db.CopyFrom(r, "COPY words FROM STDIN WITH CSV")
	if err != nil {
		panic(err)
	}

	buf := &bytes.Buffer{}
	_, err = db.CopyTo(buf, "COPY words TO STDOUT WITH CSV")
	if err != nil {
		panic(err)
	}
	fmt.Println(buf.String())
	// Output: hello,5
	// foo,3
}
