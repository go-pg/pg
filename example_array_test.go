package pg_test

import (
	"fmt"

	"gopkg.in/pg.v4"
)

func ExampleDB_Model_postgresqlArrayStructTag() {
	type Item struct {
		Id      int64
		Emails  []string `pg:",array"` // marshalled as PostgreSQL array
		Numbers []int    `pg:",array"` // marshalled as PostgreSQL array
	}

	_, err := db.Exec(`CREATE TEMP TABLE items (id serial, emails text[], numbers int[])`)
	if err != nil {
		panic(err)
	}

	item1 := Item{
		Id:      1,
		Emails:  []string{"one@example.com", "two@example.com"},
		Numbers: []int{123, 321},
	}
	if err := db.Create(&item1); err != nil {
		panic(err)
	}

	var item Item
	err = db.Model(&item).Where("id = ?", 1).Select()
	if err != nil {
		panic(err)
	}
	fmt.Println(item)
	// Output: {1 [one@example.com two@example.com] [123 321]}
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
