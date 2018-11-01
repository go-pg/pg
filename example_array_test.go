package pg_test

import (
	"fmt"

	"github.com/go-pg/pg"
	"github.com/go-pg/pg/types"
)

func ExampleDB_Model_postgresArrayStructTag() {
	type Item struct {
		Id      int64
		Emails  []string `sql:",array"` // marshalled as PostgreSQL array
		Numbers [][]int  `sql:",array"` // marshalled as PostgreSQL array
	}

	_, err := pgdb.Exec(`CREATE TEMP TABLE items (id serial, emails text[], numbers int[][])`)
	panicIf(err)
	defer pgdb.Exec("DROP TABLE items")

	item1 := Item{
		Id:      1,
		Emails:  []string{"one@example.com", "two@example.com"},
		Numbers: [][]int{{1, 2}, {3, 4}},
	}
	err = pgdb.Insert(&item1)
	panicIf(err)

	item := new(Item)
	err = pgdb.Model(item).Where("id = ?", 1).Select()
	panicIf(err)
	fmt.Println(item)
	// Output: &{1 [one@example.com two@example.com] [[1 2] [3 4]]}
}

func ExampleArray() {
	src := []string{"one@example.com", "two@example.com"}
	var dst []string
	_, err := pgdb.QueryOne(pg.Scan(pg.Array(&dst)), `SELECT ?`, pg.Array(src))
	panicIf(err)
	fmt.Println(dst)
	// Output: [one@example.com two@example.com]
}

type MyArrayValueScanner struct {
	sum int
}

var _ types.ArrayValueScanner = (*MyArrayValueScanner)(nil)

func (s *MyArrayValueScanner) BeforeScanArrayValue(rd types.Reader, n int) error {
	return nil
}

func (s *MyArrayValueScanner) ScanArrayValue(rd types.Reader, n int) error {
	num, err := types.ScanInt(rd, n)
	if err != nil {
		return err
	}
	s.sum += num
	return nil
}

func (s *MyArrayValueScanner) AfterScanArrayValue() error {
	return nil
}

func ExampleDB_arrayValueScanner() {
	var dst MyArrayValueScanner
	_, err := pgdb.QueryOne(pg.Scan(pg.Array(&dst)),
		`SELECT array_agg(id) from generate_series(0, 10) AS id`)
	panicIf(err)
	fmt.Println(dst.sum)
	// Output: 55
}
