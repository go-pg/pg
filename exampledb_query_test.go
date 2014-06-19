package pg_test

import (
	"fmt"

	"gopkg.in/pg.v2"
)

type User struct {
	Name   string
	Emails []string
}

type Users []*User

func (users *Users) New() interface{} {
	u := &User{}
	*users = append(*users, u)
	return u
}

func CreateUser(db *pg.DB, user *User) error {
	_, err := db.ExecOne(`INSERT INTO users VALUES (?name, ?emails)`, user)
	return err
}

func GetUsers(db *pg.DB) ([]*User, error) {
	var users Users
	_, err := db.Query(&users, `SELECT * FROM users`)
	if err != nil {
		return nil, err
	}
	return users, nil
}

func ExampleDB_Query() {
	db := pg.Connect(&pg.Options{
		User: "postgres",
	})
	defer db.Close()

	_, err := db.Exec(`CREATE TEMP TABLE users (name text, emails text[])`)
	if err != nil {
		panic(err)
	}

	err = CreateUser(db, &User{"admin", []string{"admin1@admin", "admin2@admin"}})
	if err != nil {
		panic(err)
	}

	err = CreateUser(db, &User{"root", []string{"root1@root", "root2@root"}})
	if err != nil {
		panic(err)
	}

	users, err := GetUsers(db)
	if err != nil {
		panic(err)
	}

	fmt.Println(users[0], users[1])
	// Output: &{admin [admin1@admin admin2@admin]} &{root [root1@root root2@root]}
}
