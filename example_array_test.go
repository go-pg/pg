package pg_test

import (
	"fmt"

	"github.com/go-pg/pg"
)

func ExampleDB_Model_postgresArrayStructTag() {
	type Item struct {
		Id      int64
		Emails  []string `pg:",array"` // marshalled as PostgreSQL array
		Numbers [][]int  `pg:",array"` // marshalled as PostgreSQL array
	}

	_, err := db.Exec(`CREATE TEMP TABLE items (id serial, emails text[], numbers int[][])`)
	if err != nil {
		panic(err)
	}
	defer db.Exec("DROP TABLE items")

	item1 := Item{
		Id:      1,
		Emails:  []string{"one@example.com", "two@example.com"},
		Numbers: [][]int{{1, 2}, {3, 4}},
	}
	if err := db.Insert(&item1); err != nil {
		panic(err)
	}

	var item Item
	err = db.Model(&item).Where("id = ?", 1).Select()
	if err != nil {
		panic(err)
	}
	fmt.Println(item)
	// Output: {1 [one@example.com two@example.com] [[1 2] [3 4]]}
}

func ExampleArray() {
	src := []string{"one@example.com", "two@example.com"}
	var dst []string
	_, err := db.QueryOne(pg.Scan(pg.Array(&dst)), `SELECT ?`, pg.Array(src))
	if err != nil {
		panic(err)
	}
	fmt.Println(dst)
	// Output: [one@example.com two@example.com]
}
