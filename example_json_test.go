package pg_test

import (
	"fmt"

	"gopkg.in/pg.v4"
)

type Item struct {
	Id   int64
	Data map[string]interface{}
}

func Example_json() {
	db := pg.Connect(&pg.Options{
		User: "postgres",
	})
	defer db.Close()

	_, err := db.Exec(`CREATE TEMP TABLE items (id serial, data json)`)
	if err != nil {
		panic(err)
	}

	item1 := Item{
		Id:   1,
		Data: map[string]interface{}{"hello": "world"},
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
	// Output: {1 map[hello:world]}
}
