package pg_test

import (
	"fmt"

	"gopkg.in/pg.v3"
)

type User struct {
	Id     int64
	Name   string
	Emails []string
}

type Users []*User

var _ pg.Collection = &Users{}

func (users *Users) NewRecord() interface{} {
	u := &User{}
	*users = append(*users, u)
	return u
}

func CreateUser(db *pg.DB, user *User) error {
	_, err := db.QueryOne(user, `
		INSERT INTO users (name, emails) VALUES (?name, ?emails)
		RETURNING id
	`, user)
	return err
}

func GetUser(db *pg.DB, id int64) (*User, error) {
	var user User
	_, err := db.QueryOne(&user, `SELECT * FROM users WHERE id = ?`, id)
	return &user, err
}

func GetUsers(db *pg.DB) ([]*User, error) {
	var users Users
	_, err := db.Query(&users, `SELECT * FROM users`)
	return users, err
}

func ExampleDB_Query() {
	db := pg.Connect(&pg.Options{
		User: "postgres",
	})

	_, err := db.Exec(`CREATE TEMP TABLE users (id serial, name text, emails text[])`)
	if err != nil {
		panic(err)
	}

	err = CreateUser(db, &User{
		Name:   "admin",
		Emails: []string{"admin1@admin", "admin2@admin"},
	})
	if err != nil {
		panic(err)
	}

	err = CreateUser(db, &User{
		Name:   "root",
		Emails: []string{"root1@root", "root2@root"},
	})
	if err != nil {
		panic(err)
	}

	user, err := GetUser(db, 1)
	if err != nil {
		panic(err)
	}

	users, err := GetUsers(db)
	if err != nil {
		panic(err)
	}

	fmt.Println(user)
	fmt.Println(users[0], users[1])
	// Output: &{1 admin [admin1@admin admin2@admin]}
	// &{1 admin [admin1@admin admin2@admin]} &{2 root [root1@root root2@root]}
}
