package pg_test

import (
	"fmt"

	"github.com/vmihailenco/pg"
)

func ExampleConnect() {
	db := pg.Connect(&pg.Options{
		User: "postgres",
	})
	defer db.Close()

	var num int
	_, err := db.QueryOne(pg.LoadInto(&num), "SELECT ?", 42)
	fmt.Println(num, err)
	// Output: 42 <nil>
}

type User struct {
	Name string
}

type Users struct {
	Values []*User
}

func (f *Users) New() interface{} {
	u := &User{}
	f.Values = append(f.Values, u)
	return u
}

func ExampleDB_Query() {
	db := pg.Connect(&pg.Options{
		User: "postgres",
	})
	defer db.Close()

	users := &Users{}
	res, err := db.Query(users, `
		WITH users (name) AS (VALUES (?), (?))
		SELECT * FROM users
	`, "admin", "root")
	fmt.Println(res.Affected(), err)
	fmt.Println(users.Values[0], users.Values[1])
	// Output: 2 <nil>
	// &{admin} &{root}
}

func ExampleDB_QueryOne() {
	db := pg.Connect(&pg.Options{
		User: "postgres",
	})
	defer db.Close()

	user := &User{}
	res, err := db.QueryOne(user, `
		WITH users (name) AS (VALUES (?))
		SELECT * FROM users
	`, "admin")
	fmt.Println(res.Affected(), err)
	fmt.Println(user)
	// Output: 1 <nil>
	// &{admin}
}
