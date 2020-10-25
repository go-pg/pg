package main

import (
	pg "github.com/go-pg/pg/v11"
	"github.com/go-pg/pg/v11/orm"
)

func init() {
	orm.RegisterTable((*ProjectDocument)(nil))
}

func main() {
	db := pg.Connect(&pg.Options{
		User: "postgres",
	})

	// if _, err := db.Model(&Project{
	// 	ID:        "bsvt22v0cr8rnl249230",
	// 	CompanyID: "a9543f45-5ea3-4bcb-81c6-1cba6c278f6d",
	// }).Insert(); err != nil {
	// 	panic(err)
	// }

	// if _, err := db.Model(&Document{
	// 	ID:        "bsvsdsdad",
	// 	CompanyID: "a9543f45-5ea3-4bcb-81c6-1cba6c278f6d",
	// }).Insert(); err != nil {
	// 	panic(err)
	// }

	// if _, err := db.Model(&ProjectDocument{
	// 	ProjectID:  "bsvt22v0cr8rnl249230",
	// 	DocumentID: "bsvsdsdad",
	// 	CompanyID:  "a9543f45-5ea3-4bcb-81c6-1cba6c278f6d",
	// }).Insert(); err != nil {
	// 	panic(err)
	// }

	qwe := &Project{
		ID:        "bsvt22v0cr8rnl249230",
		CompanyID: "a9543f45-5ea3-4bcb-81c6-1cba6c278f6d",
	}

	err := db.Model(qwe).
		WherePK().
		Relation("Documents").
		Select()
	if err != nil {
		panic(err)
	}
}

type Project struct {
	ID        string     `pg:",pk"`
	CompanyID string     `pg:",pk"`
	Documents []Document `pg:",many2many:project_documents"`
}

type Document struct {
	ID        string `pg:",pk"`
	CompanyID string `pg:",pk"`
}

type ProjectDocument struct {
	CompanyID  string `pg:",pk"`
	ProjectID  string `pg:",pk"`
	Project    *Project
	DocumentID string `pg:",pk"`
	Document   *Document
}
