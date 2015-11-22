package pg_test

import (
	"fmt"

	"gopkg.in/pg.v3"
)

type User struct {
	Id     int64 `pg:",nullempty"`
	Name   string
	Emails []string
}

func (u User) String() string {
	return fmt.Sprintf("User<%d %s %v>", u.Id, u.Name, u.Emails)
}

func CreateUser(db *pg.DB, user *User) error {
	model := pg.NewModel(user, "")
	_, err := db.QueryOne(model, `
		INSERT INTO users (?Fields) VALUES (?Values)
		RETURNING id
	`, model)
	return err
}

func GetUser(db *pg.DB, id int64) (*User, error) {
	var user User
	_, err := db.QueryOne(&user, `SELECT * FROM users WHERE id = ?`, id)
	return &user, err
}

func GetUsers(db *pg.DB) ([]User, error) {
	var users []User
	_, err := db.Query(&users, `SELECT * FROM users`)
	return users, err
}

func GetUsersByIds(db *pg.DB, ids []int64) ([]User, error) {
	var users []User
	_, err := db.Query(&users, `SELECT * FROM users WHERE id IN (?)`, pg.Ints(ids))
	return users, err
}

type Story struct {
	Id     int64 `pg:",nullempty"`
	Title  string
	UserId int64
	User   *User `pg:"-"`
}

func (s Story) String() string {
	return fmt.Sprintf("Story<%d %s %s>", s.Id, s.Title, s.User)
}

func CreateStory(db *pg.DB, story *Story) error {
	model := pg.NewModel(story, "")
	_, err := db.QueryOne(model, `
		INSERT INTO stories (?Fields) VALUES (?Values)
		RETURNING id
	`, model)
	return err
}

// GetStory returns story with associated user (author of the story).
func GetStory(db *pg.DB, id int64) (*Story, error) {
	story := &Story{
		Id: id,
	}
	model := pg.NewModel(story, "s").HasOne("User", "u")
	_, err := db.QueryOne(model, `
		SELECT ?Columns
		FROM stories AS s, users AS u
		WHERE s.id = ?id AND u.id = s.user_id
	`, model)
	return story, err
}

func createSchema(db *pg.DB) error {
	queries := []string{
		`CREATE TEMP TABLE users (id serial, name text, emails text[])`,
		`CREATE TEMP TABLE stories (id serial, title text, user_id bigint)`,
	}
	for _, q := range queries {
		_, err := db.Exec(q)
		if err != nil {
			return err
		}
	}
	return nil
}

func ExampleDB_Query() {
	db := pg.Connect(&pg.Options{
		User: "postgres",
	})

	err := createSchema(db)
	if err != nil {
		panic(err)
	}

	user1 := &User{
		Name:   "admin",
		Emails: []string{"admin1@admin", "admin2@admin"},
	}
	err = CreateUser(db, user1)
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

	story1 := &Story{
		Title:  "Cool story",
		UserId: user1.Id,
	}
	err = CreateStory(db, story1)

	user, err := GetUser(db, user1.Id)
	if err != nil {
		panic(err)
	}

	users, err := GetUsers(db)
	if err != nil {
		panic(err)
	}

	story, err := GetStory(db, story1.Id)
	if err != nil {
		panic(err)
	}
	fmt.Println(story)

	fmt.Println(user)
	fmt.Println(users[0], users[1])
	// Output: Story<1 Cool story User<1 admin [admin1@admin admin2@admin]>>
	// User<1 admin [admin1@admin admin2@admin]>
	// User<1 admin [admin1@admin admin2@admin]> User<2 root [root1@root root2@root]>
}
