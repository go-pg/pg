package main

import (
	"encoding/json"
	"fmt"

	"github.com/go-pg/pg/v10"
)

// models

type Apple struct {
	ID    string  `json:"id"`
	Balls []*Ball `json:"balls"`
}

type Ball struct {
	ID      string `json:"id"`
	AppleID string `json:"-"`
	CatID   string `json:"-"`
	Cat     *Cat   `json:"cat"`
}

type Cat struct {
	ID   string `json:"id"`
	Dogs []*Dog `json:"dogs"`
}

type Dog struct {
	ID        string      `json:"id"`
	CatID     string      `json:"-"`
	Elephants []*Elephant `json:"elephants"`
}

type Elephant struct {
	ID    string `json:"id"`
	DogID string `json:"-"`
}

// main

func main() {
	db := pg.Connect(&pg.Options{
		User: "postgres",
	})
	defer db.Close()

	var n int
	_, err := db.QueryOne(pg.Scan(&n), "SELECT 1")
	panicIf(err)

	statements := []string{
		`CREATE TEMP TABLE apples (id TEXT PRIMARY KEY)`,
		`CREATE TEMP TABLE cats (id TEXT PRIMARY KEY)`,
		`CREATE TEMP TABLE balls (id TEXT PRIMARY KEY, apple_id TEXT REFERENCES apples (id), cat_id TEXT REFERENCES cats (id))`,
		`CREATE TEMP TABLE dogs (id TEXT PRIMARY KEY, cat_id TEXT REFERENCES cats (id))`,
		`CREATE TEMP TABLE elephants (id TEXT PRIMARY KEY, dog_id TEXT REFERENCES dogs (id))`,

		`INSERT INTO apples VALUES ('apple_1')`,
		`INSERT INTO cats VALUES ('cat_1')`,
		`INSERT INTO balls VALUES ('ball_1', 'apple_1', 'cat_1'), ('ball_2', 'apple_1', 'cat_1')`,
		`INSERT INTO dogs VALUES ('dog_1', 'cat_1')`,
		`INSERT INTO elephants VALUES ('elephant_1', 'dog_1')`,
	}

	for _, s := range statements {
		_, err = db.Exec(s)
		panicIf(err)
	}

	a := &Apple{}

	err = db.Model(a).Relation("Balls.Cat.Dogs.Elephants").Select()
	panicIf(err)

	printA(a)
}

// helpers

func panicIf(err error) {
	if err != nil {
		panic(err)
	}
}

func printA(a *Apple) {
	b, err := json.MarshalIndent(a, "", "  ")
	panicIf(err)
	fmt.Println(string(b))
}
