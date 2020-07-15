package main

import (
	"log"

	"github.com/go-pg/pg/v10"
)

type Test struct {
	ID   string `json:"id" pg:",pk"`
	Data *Data  `pg:"type:jsonb"`
}

type Data struct {
	Key    string `json:"key"`
	Value  string `json:"value"`
	DataID string `json:"data_id"`
	TestID string `json:"test_id"`
}

func main() {
	pgConfig := pg.Options{
		Addr:     "localhost:5432",
		User:     "postgres",
		Password: "postgres",
		Database: "test",
	}

	db := pg.Connect(&pgConfig)
	defer db.Close()

	createSchema(db)

	t := Test{
		Data: &Data{
			Key:    "akey",
			Value:  "avalue",
			DataID: "1",
			TestID: "2",
		},
	}

	if err := db.Insert(&t); err != nil {
		log.Fatal(err)
	}
}

func createSchema(db *pg.DB) {
	queries := []string{
		`DROP TABLE IF EXISTS "tests";`,
		`CREATE TABLE "tests" (
			"id" bigserial PRIMARY KEY,
			"data" jsonb
		  );
		  `,
	}
	for _, q := range queries {
		_, err := db.Exec(q)
		if err != nil {
			log.Fatalln(err.Error())
		}
	}
}
