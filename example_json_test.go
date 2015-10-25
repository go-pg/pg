package pg_test

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"

	"gopkg.in/pg.v3"
)

type jsonMap map[string]interface{}

func (m *jsonMap) Scan(value interface{}) error {
	return json.Unmarshal(value.([]byte), m)
}

func (m jsonMap) Value() (driver.Value, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	return string(b), nil
}

type Item struct {
	Id   int64
	Data jsonMap
}

func CreateItem(db *pg.DB, item *Item) error {
	_, err := db.ExecOne(`INSERT INTO items VALUES (?id, ?data)`, item)
	return err
}

func GetItem(db *pg.DB, id int64) (*Item, error) {
	var item Item
	_, err := db.QueryOne(&item, `
		SELECT * FROM items WHERE id = ?
	`, id)
	return &item, err
}

func GetItems(db *pg.DB) ([]Item, error) {
	var items []Item
	_, err := db.Query(&items, `
		SELECT * FROM items
	`)
	return items, err
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

	item := &Item{
		Id:   1,
		Data: jsonMap{"hello": "world"},
	}
	if err := CreateItem(db, item); err != nil {
		panic(err)
	}

	item, err = GetItem(db, 1)
	if err != nil {
		panic(err)
	}
	fmt.Println(item)
	// Output: &{1 map[hello:world]}
}
