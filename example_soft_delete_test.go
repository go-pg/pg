package pg_test

import (
	"fmt"
	"time"
)

type Flight struct {
	Id        int
	Name      string
	DeletedAt time.Time `pg:",soft_delete"`
}

func ExampleDB_Model_softDelete() {
	flight1 := &Flight{
		Id: 1,
	}
	_, err := pgdb.Model(flight1).Insert()
	panicIf(err)

	// Soft delete.
	_, err = pgdb.Model(flight1).WherePK().Delete()
	panicIf(err)

	// Count visible flights.
	count, err := pgdb.Model((*Flight)(nil)).Count()
	panicIf(err)
	fmt.Println("count", count)

	// Count soft deleted flights.
	deletedCount, err := pgdb.Model((*Flight)(nil)).Deleted().Count()
	panicIf(err)
	fmt.Println("deleted count", deletedCount)

	// Actually delete the flight.
	_, err = pgdb.Model(flight1).WherePK().ForceDelete()
	panicIf(err)

	// Count soft deleted flights.
	deletedCount, err = pgdb.Model((*Flight)(nil)).Deleted().Count()
	panicIf(err)
	fmt.Println("deleted count", deletedCount)

	// Output: count 0
	// deleted count 1
	// deleted count 0
}
