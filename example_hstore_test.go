package pg_test

import (
	"fmt"

	"github.com/go-pg/pg/v11"
)

func ExampleDB_Model_hstoreStructTag() {
	type Item struct {
		Id    int64
		Attrs map[string]string `pg:",hstore"` // marshalled as PostgreSQL hstore
	}

	_, err := pgdb.Exec(ctx, `CREATE TEMP TABLE items (id serial, attrs hstore)`)
	if err != nil {
		panic(err)
	}
	defer pgdb.Exec(ctx, "DROP TABLE items")

	item1 := Item{
		Id:    1,
		Attrs: map[string]string{"hello": "world"},
	}
	_, err = pgdb.Model(&item1).Insert(ctx)
	if err != nil {
		panic(err)
	}

	var item Item
	err = pgdb.Model(&item).Where("id = ?", 1).Select(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Println(item)
	// Output: {1 map[hello:world]}
}

func ExampleHstore() {
	src := map[string]string{"hello": "world"}
	var dst map[string]string
	_, err := pgdb.QueryOne(ctx, pg.Scan(pg.Hstore(&dst)), `SELECT ?`, pg.Hstore(src))
	if err != nil {
		panic(err)
	}
	fmt.Println(dst)
	// Output: map[hello:world]
}
