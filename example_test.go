package pg_test

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	"gopkg.in/pg.v5"
	"gopkg.in/pg.v5/orm"
)

var db *pg.DB

func init() {
	db = connect()
}

func connect() *pg.DB {
	return pg.Connect(pgOptions())
}

func ExampleConnect() {
	db := pg.Connect(&pg.Options{
		User:     "postgres",
		Password: "",
		Database: "postgres",
	})

	var n int
	_, err := db.QueryOne(pg.Scan(&n), "SELECT 1")
	if err != nil {
		panic(err)
	}
	fmt.Println(n)

	err = db.Close()
	if err != nil {
		panic(err)
	}

	// Output: 1
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
	fmt.Println(res.RowsAffected())
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
    `, &user)
	if err != nil {
		panic(err)
	}
	fmt.Println(user)
	// Output: {1 admin}
}

func ExampleDB_Exec() {
	res, err := db.Exec(`CREATE TEMP TABLE test()`)
	fmt.Println(res.RowsAffected(), err)
	// Output: -1 <nil>
}

func ExampleListener() {
	ln := db.Listen("mychan")
	defer ln.Close()

	ch := ln.Channel()

	go func() {
		_, err := db.Exec("NOTIFY mychan, ?", "hello world")
		if err != nil {
			panic(err)
		}
	}()

	notif := <-ch
	fmt.Println(notif)
	// Output: &{mychan hello world}
}

func txExample() *pg.DB {
	db := pg.Connect(&pg.Options{
		User: "postgres",
	})

	queries := []string{
		`DROP TABLE IF EXISTS tx_test`,
		`CREATE TABLE tx_test(counter int)`,
		`INSERT INTO tx_test (counter) VALUES (0)`,
	}
	for _, q := range queries {
		_, err := db.Exec(q)
		if err != nil {
			panic(err)
		}
	}

	return db
}

func ExampleDB_Begin() {
	db := txExample()

	tx, err := db.Begin()
	if err != nil {
		panic(err)
	}

	var counter int
	_, err = tx.QueryOne(pg.Scan(&counter), `SELECT counter FROM tx_test`)
	if err != nil {
		tx.Rollback()
		panic(err)
	}

	counter++

	_, err = tx.Exec(`UPDATE tx_test SET counter = ?`, counter)
	if err != nil {
		tx.Rollback()
		panic(err)
	}

	err = tx.Commit()
	if err != nil {
		panic(err)
	}

	fmt.Println(counter)
	// Output: 1
}

func ExampleDB_RunInTransaction() {
	db := txExample()

	var counter int
	// Transaction is automatically rollbacked on error.
	err := db.RunInTransaction(func(tx *pg.Tx) error {
		_, err := tx.QueryOne(pg.Scan(&counter), `SELECT counter FROM tx_test`)
		if err != nil {
			return err
		}

		counter++

		_, err = tx.Exec(`UPDATE tx_test SET counter = ?`, counter)
		return err
	})
	if err != nil {
		panic(err)
	}

	fmt.Println(counter)
	// Output: 1
}

func ExampleDB_Prepare() {
	stmt, err := db.Prepare(`SELECT $1::text, $2::text`)
	if err != nil {
		panic(err)
	}

	var s1, s2 string
	_, err = stmt.QueryOne(pg.Scan(&s1, &s2), "foo", "bar")
	if err != nil {
		panic(err)
	}
	fmt.Println(s1, s2)
	// Output: foo bar
}

func ExampleDB_CreateTable() {
	type Model struct {
		Id   int
		Name string
	}

	err := db.CreateTable(&Model{}, &orm.CreateTableOptions{
		Temp: true, // create temp table
	})
	if err != nil {
		panic(err)
	}

	var info []struct {
		ColumnName string
		DataType   string
	}
	_, err = db.Query(&info, `
		SELECT column_name, data_type
		FROM information_schema.columns
		WHERE table_name = 'models'
	`)
	if err != nil {
		panic(err)
	}
	fmt.Println(info)
	// Output: [{id bigint} {name text}]
}

func ExampleInts() {
	var nums pg.Ints
	_, err := db.Query(&nums, `SELECT generate_series(0, 10)`)
	fmt.Println(nums, err)
	// Output: [0 1 2 3 4 5 6 7 8 9 10] <nil>
}

func ExampleStrings() {
	var strs pg.Strings
	_, err := db.Query(&strs, `
		WITH users AS (VALUES ('foo'), ('bar')) SELECT * FROM users
	`)
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

	var buf bytes.Buffer
	_, err = db.CopyTo(&buf, `COPY words TO STDOUT WITH CSV`)
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

func ExampleScan() {
	var s1, s2 string
	_, err := db.QueryOne(pg.Scan(&s1, &s2), `SELECT ?, ?`, "foo", "bar")
	fmt.Println(s1, s2, err)
	// Output: foo bar <nil>
}
