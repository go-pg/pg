package pg_test

import (
	"fmt"

	"github.com/go-pg/pg"
	"github.com/go-pg/pg/orm"
)

func init() {
	// Register many to many model so ORM can better recognize m2m relation.
	// This should be done before dependant models are used.
	orm.RegisterTable((*ElemToElem)(nil))
}

type Elem struct {
	Id    int
	Elems []Elem `pg:"many2many:elem_to_elems,joinFK:sub_id"`
}

type ElemToElem struct {
	ElemId int
	SubId  int
}

func createManyToManySefTables(db *pg.DB) error {
	models := []interface{}{
		(*Elem)(nil),
		(*ElemToElem)(nil),
	}
	for _, model := range models {
		err := db.CreateTable(model, &orm.CreateTableOptions{
			Temp: true,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func ExampleDB_Model_manyToManySelf() {
	db := connect()
	defer db.Close()

	if err := createManyToManySefTables(db); err != nil {
		panic(err)
	}

	values := []interface{}{
		&Elem{Id: 1},
		&Elem{Id: 2},
		&Elem{Id: 3},
		&ElemToElem{ElemId: 1, SubId: 2},
		&ElemToElem{ElemId: 1, SubId: 3},
	}
	for _, v := range values {
		err := db.Insert(v)
		if err != nil {
			panic(err)
		}
	}

	// Select elem and all subelems with following queries:
	//
	// SELECT "elem"."id" FROM "elems" AS "elem" ORDER BY "elem"."id" LIMIT 1
	//
	// SELECT elem_to_elems.*, "elem"."id" FROM "elems" AS "elem"
	// JOIN elem_to_elems AS elem_to_elems ON (elem_to_elems."elem_id") IN (1)
	// WHERE ("elem"."id" = elem_to_elems."sub_id")

	elem := new(Elem)
	err := db.Model(elem).Relation("Elems").First()
	if err != nil {
		panic(err)
	}
	fmt.Println("Elem", elem.Id)
	fmt.Println("Subelems", elem.Elems[0].Id, elem.Elems[1].Id)
	// Output: Elem 1
	// Subelems 2 3
}
