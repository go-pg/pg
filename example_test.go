package pg_test

import (
	"fmt"

	"github.com/vmihailenco/pg"
)

func ExampleConnect() {
	db := pg.Connect(&pg.Options{
		User: "postgres",
	})
	defer db.Close()
}

func ExampleDB_QueryOne() {
	db := pg.Connect(&pg.Options{
		User: "postgres",
	})
	defer db.Close()

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
	db := pg.Connect(&pg.Options{
		User: "postgres",
	})
	defer db.Close()

	res, err := db.Exec("CREATE TEMP TABLE test()")
	fmt.Println(res.Affected(), err)
	// Output: 0 <nil>
}

func ExampleLoadInto() {
	db := pg.Connect(&pg.Options{
		User: "postgres",
	})
	defer db.Close()

	var s1, s2 string
	_, err := db.QueryOne(pg.LoadInto(&s1, &s2), "SELECT ?, ?", "foo", "bar")
	fmt.Println(s1, s2, err)
	// Output: foo bar <nil>
}

func ExampleListener() {
	db := pg.Connect(&pg.Options{
		User: "postgres",
	})
	defer db.Close()

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
