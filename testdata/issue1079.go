package main

import (
	"github.com/go-pg/pg"
	"github.com/go-pg/pg/orm"
)

type MyType struct {
	MyInfo [3]bool `sql:",array"`
}

func createSchema(db *pg.DB) error {
	for _, model := range []interface{}{(*MyType)(nil)} {
		err := db.CreateTable(model, &orm.CreateTableOptions{
			Temp: true,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func main() {
	db := pg.Connect(&pg.Options{
		User: "postgres",
	})
	defer db.Close()

	err := createSchema(db)
	if err != nil {
		panic(err)
	}

	thing := &MyType{
		MyInfo: [3]bool{true, false, true},
	}
	err = db.Insert(thing)
	if err != nil {
		panic(err)
	}

	thing2 := new(MyType)
	err = db.Model(thing2).Select()
	if err != nil {
		panic(err)
	}

	if thing2.MyInfo != thing.MyInfo {
		panic("not equal")
	}
}
