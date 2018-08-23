package pg_test

import (
	"fmt"
	"time"

	"github.com/go-pg/pg/orm"
)

type Flight struct {
	Id        int
	DeletedAt time.Time
}

func ExampleDB_Model_softDelete() {
	db := connect()
	defer db.Close()

	err := db.DropTable((*Flight)(nil), &orm.DropTableOptions{
		IfExists: true,
		Cascade:  true,
	})
	panicIf(err)

	err = db.CreateTable((*Flight)(nil), nil)
	panicIf(err)

	flight1 := &Flight{
		Id: 1,
	}
	err = db.Insert(flight1)
	panicIf(err)

	err = db.Delete(flight1)
	panicIf(err)

	count, err := db.Model((*Flight)(nil)).Count()
	panicIf(err)
	fmt.Println("count", count)

	deletedCount, err := db.Model((*Flight)(nil)).Deleted().Count()
	panicIf(err)
	fmt.Println("deleted count", deletedCount)

	err = db.ForceDelete(flight1)
	panicIf(err)

	deletedCount, err = db.Model((*Flight)(nil)).Deleted().Count()
	panicIf(err)
	fmt.Println("deleted count", deletedCount)

	// Output: count 0
	// deleted count 1
	// deleted count 0
}
