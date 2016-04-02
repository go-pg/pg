package pg_test

import (
	"fmt"

	"gopkg.in/pg.v4"
)

func ExampleDB_Create() {
	db := connectDB()

	book := Book{
		Title:    "new book",
		AuthorID: 1,
	}

	err := db.Create(&book)
	if err != nil {
		panic(err)
	}
	fmt.Println(book)
	// Output: Book<Id=4 Title="new book">

	err = db.Delete(&book)
	if err != nil {
		panic(err)
	}
}

func ExampleDB_Model_selectFirstAndLastRow() {
	db := connectDB()

	var firstBook Book
	err := db.Model(&firstBook).First()
	if err != nil {
		panic(err)
	}

	var lastBook Book
	err = db.Model(&lastBook).Last()
	if err != nil {
		panic(err)
	}

	fmt.Println(firstBook, lastBook)
	// Output: Book<Id=1 Title="book 1"> Book<Id=3 Title="book 3">
}

func ExampleDB_Model_selectAllColumn() {
	db := connectDB()

	var book Book
	err := db.Model(&book).Column("book.*").First()
	if err != nil {
		panic(err)
	}
	fmt.Println(book, book.AuthorID)
	// Output: Book<Id=1 Title="book 1"> 10
}

func ExampleDB_Model_selectSomeColumns() {
	db := connectDB()

	var book Book
	err := db.Model(&book).
		Column("book.id").
		First()
	if err != nil {
		panic(err)
	}

	fmt.Println(book)
	// Output: Book<Id=1 Title="">
}

func ExampleDB_Model_countRows() {
	db := connectDB()

	count, err := db.Model(Book{}).Count()
	if err != nil {
		panic(err)
	}

	fmt.Println(count)
	// Output: 3
}

func ExampleDB_Update() {
	db := connectDB()

	err := db.Update(&Book{
		Id:    1,
		Title: "updated book 1",
	})
	if err != nil {
		panic(err)
	}

	var book Book
	err = db.Model(&book).Where("id = ?", 1).Select()
	if err != nil {
		panic(err)
	}

	fmt.Println(book)
	// Output: Book<Id=1 Title="updated book 1">
}

func ExampleDB_Update_someColumns() {
	db := connectDB()

	book := Book{
		Id:       1,
		Title:    "updated book 1",
		AuthorID: 2,
	}
	err := db.Model(&book).Column("title").Returning("*").Update()
	if err != nil {
		panic(err)
	}

	fmt.Println(book, book.AuthorID)
	// Output: Book<Id=1 Title="updated book 1"> 10
}

func ExampleDB_Update_usingSqlFunction() {
	db := connectDB()

	id := 1
	data := map[string]interface{}{
		"title": pg.Q("concat(?, title, ?)", "prefix ", " suffix"),
	}
	var book Book
	err := db.Model(&book).
		Where("id = ?", id).
		Returning("*").
		UpdateValues(data)
	if err != nil {
		panic(err)
	}

	fmt.Println(book)
	// Output: Book<Id=1 Title="prefix book 1 suffix">
}

func ExampleDB_Update_multipleRows() {
	db := connectDB()

	ids := pg.Ints{1, 2}
	data := map[string]interface{}{
		"title": pg.Q("concat(?, title, ?)", "prefix ", " suffix"),
	}

	var books []Book
	err := db.Model(&books).
		Where("id IN (?)", ids).
		Returning("*").
		UpdateValues(data)
	if err != nil {
		panic(err)
	}

	fmt.Println(books[0], books[1])
	// Output: Book<Id=1 Title="prefix book 1 suffix"> Book<Id=2 Title="prefix book 2 suffix">
}

func ExampleDB_Delete() {
	db := connectDB()

	book := Book{
		Title:    "title 1",
		AuthorID: 1,
	}
	err := db.Create(&book)
	if err != nil {
		panic(err)
	}

	err = db.Delete(book)
	if err != nil {
		panic(err)
	}

	err = db.Model(&Book{}).Where("id = ?", book.Id).First()
	fmt.Println(err)
	// Output: pg: no rows in result set
}

func ExampleDB_Delete_multipleRows() {
	db := connectDB()

	ids := pg.Ints{1, 2, 3}
	err := db.Model(Book{}).Where("id IN (?)", ids).Delete()
	if err != nil {
		panic(err)
	}

	count, err := db.Model(Book{}).Count()
	if err != nil {
		panic(err)
	}

	fmt.Println(count)
	// Output: 0
}
