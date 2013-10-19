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
