package pg_test

import (
	"fmt"

	"github.com/go-pg/pg"
)

func ExampleDB_Model_hstoreStructTag() {
	type Item struct {
		Id    int64
		Attrs map[string]string `sql:",hstore"` // marshalled as PostgreSQL hstore
	}

	_, err := pgdb.Exec(`CREATE TEMP TABLE items (id serial, attrs hstore)`)
	if err != nil {
		panic(err)
	}
	defer pgdb.Exec("DROP TABLE items")

	item1 := Item{
		Id:    1,
		Attrs: map[string]string{"hello": "world"},
	}
	err = pgdb.Insert(&item1)
	if err != nil {
		panic(err)
	}

	var item Item
	err = pgdb.Model(&item).Where("id = ?", 1).Select()
	if err != nil {
		panic(err)
	}
	fmt.Println(item)
	// Output: {1 map[hello:world]}
}

func ExampleHstore() {
	src := map[string]string{"hello": "world"}
	var dst map[string]string
	_, err := pgdb.QueryOne(pg.Scan(pg.Hstore(&dst)), `SELECT ?`, pg.Hstore(src))
	if err != nil {
		panic(err)
	}
	fmt.Println(dst)
	// Output: map[hello:world]
}
