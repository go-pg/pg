package pg_test

import (
	"fmt"

	"gopkg.in/pg.v5"
)

type Params struct {
	X int
	Y int
}

func (p *Params) Sum() int {
	return p.X + p.Y
}

// go-pg recognizes placeholders (`?`) in queries and replaces them
// with parameters when queries are executed. Parameters are escaped
// before replacing according to PostgreSQL rules. Specifically:
// - all parameters are properly quoted against SQL injections;
// - null byte is removed;
// - JSON/JSONB gets `\u0000` escaped as `\\u0000`.
func ExampleDB_Placeholders() {
	var num int

	// Simple placeholders.
	_, err := db.Query(pg.Scan(&num), "SELECT ?", 42)
	if err != nil {
		panic(err)
	}
	fmt.Println(num)

	// Indexed placeholders.
	_, err = db.Query(pg.Scan(&num), "SELECT ?0 + ?0", 1)
	if err != nil {
		panic(err)
	}
	fmt.Println(num)

	// Named placeholders.
	params := &Params{
		X: 1,
		Y: 1,
	}
	_, err = db.Query(pg.Scan(&num), "SELECT ?x + ?y + ?Sum", &params)
	if err != nil {
		panic(err)
	}
	fmt.Println(num)

	// Output: 42
	// 2
	// 4
}
