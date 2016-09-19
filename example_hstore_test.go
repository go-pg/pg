package pg_test

import (
	"fmt"

	"gopkg.in/pg.v5"
)

func ExampleDB_Model_hstoreStructTag() {
	type Item struct {
		Id    int64
		Attrs map[string]string `pg:",hstore"` // marshalled as PostgreSQL hstore
	}

	_, err := db.Exec(`CREATE TEMP TABLE items (id serial, attrs hstore)`)
	if err != nil {
		panic(err)
	}
	defer db.Exec("DROP TABLE items")

	item1 := Item{
		Id:    1,
		Attrs: map[string]string{"hello": "world"},
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
	// Output: {1 map[hello:world]}
}

func ExampleHstore() {
	src := map[string]string{"hello": "world"}
	var dst map[string]string
	_, err := db.QueryOne(pg.Scan(pg.Hstore(&dst)), `SELECT ?`, pg.Hstore(src))
	if err != nil {
		panic(err)
	}
	fmt.Println(dst)
	// Output: map[hello:world]
}
