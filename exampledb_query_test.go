package pg_test

import (
	"fmt"

	"github.com/vmihailenco/pg"
)

type User struct {
	Name   string
	Emails []string
}

type Users struct {
	Values []*User
}

func (f *Users) New() interface{} {
	u := &User{}
	f.Values = append(f.Values, u)
	return u
}

func GetUsers(db *pg.DB) ([]*User, error) {
	users := &Users{}
	_, err := db.Query(users,
		`WITH users (name, emails) AS (VALUES (?, ?), (?, ?))
		SELECT * FROM users`,
		"admin", []string{"admin1@admin", "admin2@admin"},
		"root", []string{"root1@root", "root2@root"},
	)
	if err != nil {
		return nil, err
	}
	return users.Values, nil
}

func ExampleDB_Query() {
	db := pg.Connect(&pg.Options{
		User: "postgres",
	})
	defer db.Close()

	users, err := GetUsers(db)
	fmt.Println(err)
	fmt.Println(users[0], users[1])
	// Output: <nil>
	// &{admin [admin1@admin admin2@admin]} &{root [root1@root root2@root]}
}
