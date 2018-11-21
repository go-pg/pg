package pg_test

import (
	"fmt"

	"github.com/go-pg/pg"
)

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

func GetUsers(db *pg.DB) ([]User, error) {
	var users []User
	_, err := db.Query(&users, `SELECT * FROM users`)
	return users, err
}

func GetUsersByIds(db *pg.DB, ids []int64) ([]User, error) {
	var users []User
	_, err := db.Query(&users, `SELECT * FROM users WHERE id IN (?)`, pg.In(ids))
	return users, err
}

func CreateStory(db *pg.DB, story *Story) error {
	_, err := db.QueryOne(story, `
		INSERT INTO stories (title, author_id) VALUES (?title, ?author_id)
		RETURNING id
	`, story)
	return err
}

// GetStory returns story with associated author.
func GetStory(db *pg.DB, id int64) (*Story, error) {
	var story Story
	_, err := db.QueryOne(&story, `
		SELECT s.*,
			u.id AS author__id, u.name AS author__name, u.emails AS author__emails
		FROM stories AS s, users AS u
		WHERE s.id = ? AND u.id = s.author_id
	`, id)
	return &story, err
}

func ExampleDB_Query() {
	db := pg.Connect(&pg.Options{
		User: "postgres",
	})

	err := createSchema(db)
	panicIf(err)

	user1 := &User{
		Name:   "admin",
		Emails: []string{"admin1@admin", "admin2@admin"},
	}
	err = CreateUser(db, user1)
	panicIf(err)

	err = CreateUser(db, &User{
		Name:   "root",
		Emails: []string{"root1@root", "root2@root"},
	})
	panicIf(err)

	story1 := &Story{
		Title:    "Cool story",
		AuthorId: user1.Id,
	}
	err = CreateStory(db, story1)
	panicIf(err)

	user, err := GetUser(db, user1.Id)
	panicIf(err)

	users, err := GetUsers(db)
	panicIf(err)

	story, err := GetStory(db, story1.Id)
	panicIf(err)

	fmt.Println(user)
	fmt.Println(users)
	fmt.Println(story)
	// Output: User<1 admin [admin1@admin admin2@admin]>
	// [User<1 admin [admin1@admin admin2@admin]> User<2 root [root1@root root2@root]>]
	// Story<1 Cool story User<1 admin [admin1@admin admin2@admin]>>
}
