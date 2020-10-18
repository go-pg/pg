package pg_test

import (
	"context"
	"fmt"

	"github.com/go-pg/pg/v10"
)

func CreateUser(ctx context.Context, db *pg.DB, user *User) error {
	_, err := db.QueryOne(ctx, user, `
		INSERT INTO users (name, emails) VALUES (?name, ?emails)
		RETURNING id
	`, user)
	return err
}

func GetUser(ctx context.Context, db *pg.DB, id int64) (*User, error) {
	var user User
	_, err := db.QueryOne(ctx, &user, `SELECT * FROM users WHERE id = ?`, id)
	return &user, err
}

func GetUsers(ctx context.Context, db *pg.DB) ([]User, error) {
	var users []User
	_, err := db.Query(ctx, &users, `SELECT * FROM users`)
	return users, err
}

func GetUsersByIds(ctx context.Context, db *pg.DB, ids []int64) ([]User, error) {
	var users []User
	_, err := db.Query(ctx, &users, `SELECT * FROM users WHERE id IN (?)`, pg.In(ids))
	return users, err
}

func CreateStory(ctx context.Context, db *pg.DB, story *Story) error {
	_, err := db.QueryOne(ctx, story, `
		INSERT INTO stories (title, author_id) VALUES (?title, ?author_id)
		RETURNING id
	`, story)
	return err
}

// GetStory returns story with associated author.
func GetStory(ctx context.Context, db *pg.DB, id int64) (*Story, error) {
	var story Story
	_, err := db.QueryOne(ctx, &story, `
		SELECT s.*,
			u.id AS author__id, u.name AS author__name, u.emails AS author__emails
		FROM stories AS s, users AS u
		WHERE s.id = ? AND u.id = s.author_id
	`, id)
	return &story, err
}

func ExampleDB_Query() {
	ctx := context.Background()
	db := pg.Connect(&pg.Options{
		User: "postgres",
	})

	err := createSchema(ctx, db)
	panicIf(err)

	user1 := &User{
		Name:   "admin",
		Emails: []string{"admin1@admin", "admin2@admin"},
	}
	err = CreateUser(ctx, db, user1)
	panicIf(err)

	err = CreateUser(ctx, db, &User{
		Name:   "root",
		Emails: []string{"root1@root", "root2@root"},
	})
	panicIf(err)

	story1 := &Story{
		Title:    "Cool story",
		AuthorId: user1.Id,
	}
	err = CreateStory(ctx, db, story1)
	panicIf(err)

	user, err := GetUser(ctx, db, user1.Id)
	panicIf(err)

	users, err := GetUsers(ctx, db)
	panicIf(err)

	story, err := GetStory(ctx, db, story1.Id)
	panicIf(err)

	fmt.Println(user)
	fmt.Println(users)
	fmt.Println(story)
	// Output: User<1 admin [admin1@admin admin2@admin]>
	// [User<1 admin [admin1@admin admin2@admin]> User<2 root [root1@root root2@root]>]
	// Story<1 Cool story User<1 admin [admin1@admin admin2@admin]>>
}
