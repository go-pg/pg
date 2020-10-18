package pg_test

import (
	"fmt"

	"github.com/go-pg/pg/v10/orm"
)

type InventoryItem struct {
	Name       string
	SupplierID int
	Price      float64
}

type OnHand struct {
	tableName struct{} `pg:"on_hand"`

	Item  InventoryItem `pg:"composite:inventory_item"`
	Count int
}

func ExampleDB_Model_compositeType() {
	db := connect()
	defer db.Close(ctx)

	err := db.Model((*OnHand)(nil)).DropTable(ctx, &orm.DropTableOptions{
		IfExists: true,
		Cascade:  true,
	})
	panicIf(err)

	err = db.Model((*InventoryItem)(nil)).DropComposite(ctx, &orm.DropCompositeOptions{
		IfExists: true,
	})
	panicIf(err)

	err = db.Model((*InventoryItem)(nil)).CreateComposite(ctx, nil)
	panicIf(err)

	err = db.Model((*OnHand)(nil)).CreateTable(ctx, nil)
	panicIf(err)

	_, err = db.Model(&OnHand{
		Item: InventoryItem{
			Name:       "fuzzy dice",
			SupplierID: 42,
			Price:      1.99,
		},
		Count: 1000,
	}).Insert(ctx)
	panicIf(err)

	onHand := new(OnHand)
	err = db.Model(onHand).Select(ctx)
	panicIf(err)

	fmt.Println(onHand.Item.Name, onHand.Item.Price, onHand.Count)
	// Output: fuzzy dice 1.99 1000
}
