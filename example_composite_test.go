package pg_test

import (
	"fmt"

	"github.com/go-pg/pg/orm"
)

type InventoryItem struct {
	Name       string
	SupplierID int
	Price      float64
}

type OnHand struct {
	tableName struct{} `sql:"on_hand"`

	Item  InventoryItem `sql:"composite:inventory_item"`
	Count int
}

func ExampleDB_Model_compositeType() {
	db := connect()
	defer db.Close()

	err := db.DropTable((*OnHand)(nil), &orm.DropTableOptions{
		IfExists: true,
		Cascade:  true,
	})
	panicIf(err)

	err = db.DropComposite((*InventoryItem)(nil), &orm.DropCompositeOptions{
		IfExists: true,
	})
	panicIf(err)

	err = db.CreateComposite((*InventoryItem)(nil), nil)
	panicIf(err)

	err = db.CreateTable((*OnHand)(nil), nil)
	panicIf(err)

	err = db.Insert(&OnHand{
		Item: InventoryItem{
			Name:       "fuzzy dice",
			SupplierID: 42,
			Price:      1.99,
		},
		Count: 1000,
	})
	panicIf(err)

	onHand := new(OnHand)
	err = db.Model(onHand).Select()
	panicIf(err)

	fmt.Println(onHand.Item.Name, onHand.Item.Price, onHand.Count)
	// Output: fuzzy dice 1.99 1000
}
