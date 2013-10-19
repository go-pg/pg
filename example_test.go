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

	var num int
	_, err := db.QueryOne(pg.LoadInto(&num), "SELECT ?", 42)
	fmt.Println(num, err)
	// Output: 42 <nil>
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
