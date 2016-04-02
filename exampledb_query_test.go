package pg_test

import (
	"fmt"

	"gopkg.in/pg.v4"
)

type User struct {
	Id     int64
	Name   string
	Emails []string
}

func (u User) String() string {
	return fmt.Sprintf("User<%d %s %v>", u.Id, u.Name, u.Emails)
}

type Story struct {
	Id     int64
	Title  string
	UserId int64
	User   *User
}

func (s Story) String() string {
	return fmt.Sprintf("Story<%d %s %s>", s.Id, s.Title, s.User)
}

func createSchema(db *pg.DB) error {
	queries := []string{
		`CREATE TEMP TABLE users (id serial, name text, emails jsonb)`,
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
	err = db.Create(user1)
	if err != nil {
		panic(err)
	}

	err = db.Create(&User{
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
	err = db.Create(story1)
	if err != nil {
		panic(err)
	}

	var user User
	err = db.Model(&user).Where("id = ?", user1.Id).Select()
	if err != nil {
		panic(err)
	}

	var users []User
	err = db.Model(&users).Select()
	if err != nil {
		panic(err)
	}

	var story Story
	err = db.Model(&story).
		Column("story.*", "User").
		Where("story.id = ?", story1.Id).
		Select()
	if err != nil {
		panic(err)
	}

	fmt.Println(user)
	fmt.Println(users[0], users[1])
	fmt.Println(story)
	// Output: User<1 admin [admin1@admin admin2@admin]>
	// User<1 admin [admin1@admin admin2@admin]> User<2 root [root1@root root2@root]>
	// Story<1 Cool story User<1 admin [admin1@admin admin2@admin]>>
}
