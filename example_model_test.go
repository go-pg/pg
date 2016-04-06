package pg_test

import (
	"database/sql"
	"fmt"
	"time"

	"gopkg.in/pg.v4"
)

func modelDB() *pg.DB {
	db := pg.Connect(&pg.Options{
		User: "postgres",
	})

	err := createTestSchema(db)
	if err != nil {
		panic(err)
	}

	err = db.Create(&Author{
		Name: "author 1",
	})

	err = db.Create(&Book{
		Title:     "book 1",
		AuthorID:  1,
		EditorID:  11,
		CreatedAt: time.Now(),
	})
	if err != nil {
		panic(err)
	}

	err = db.Create(&Book{
		Title:     "book 2",
		AuthorID:  1,
		EditorID:  12,
		CreatedAt: time.Now(),
	})
	if err != nil {
		panic(err)
	}

	err = db.Create(&Book{
		Title:     "book 3",
		AuthorID:  11,
		EditorID:  11,
		CreatedAt: time.Now(),
	})
	if err != nil {
		panic(err)
	}

	for i := 0; i < 2; i++ {
		genre := Genre{
			Name: fmt.Sprintf("genre %d", i+1),
		}
		err = db.Create(&genre)
		if err != nil {
			panic(err)
		}

		err = db.Create(&BookGenre{
			BookId:  1,
			GenreId: genre.Id,
		})
		if err != nil {
			panic(err)
		}
	}

	return db
}

func ExampleDB_Create() {
	db := modelDB()

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

func ExampleDB_Create_onConflict() {
	db := modelDB()

	book := Book{
		Id:    100,
		Title: "book 100",
	}

	for i := 0; i < 2; i++ {
		res, err := db.Model(&book).OnConflict("DO NOTHING").Create()
		if err != nil {
			panic(err)
		}
		if res.Affected() > 0 {
			fmt.Println("created")
		} else {
			fmt.Println("did nothing")
		}
	}

	err := db.Delete(&book)
	if err != nil {
		panic(err)
	}

	// Output: created
	// did nothing
}

func ExampleDB_Create_getOrCreate() {
	db := modelDB()

	author := Author{
		Name: "R. Scott Bakker",
	}
	created, err := db.Model(&author).
		Column("id").
		Where("name = ?name").
		OnConflict("DO NOTHING").
		Returning("id").
		SelectOrCreate()
	if err != nil {
		panic(err)
	}
	fmt.Println(created, author)
	// Output: true Author<ID=2 Name="R. Scott Bakker">
}

func ExampleDB_Model_firstRow() {
	db := modelDB()

	var firstBook Book
	err := db.Model(&firstBook).First()
	if err != nil {
		panic(err)
	}
	fmt.Println(firstBook)
	// Output: Book<Id=1 Title="book 1">
}

func ExampleDB_Model_lastRow() {
	db := modelDB()

	var lastBook Book
	err := db.Model(&lastBook).Last()
	if err != nil {
		panic(err)
	}
	fmt.Println(lastBook)
	// Output: Book<Id=3 Title="book 3">
}

func ExampleDB_Model_selectAllColumns() {
	db := modelDB()

	var book Book
	err := db.Model(&book).Column("book.*").First()
	if err != nil {
		panic(err)
	}
	fmt.Println(book, book.AuthorID)
	// Output: Book<Id=1 Title="book 1"> 1
}

func ExampleDB_Model_selectSomeColumns() {
	db := modelDB()

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
	db := modelDB()

	count, err := db.Model(Book{}).Count()
	if err != nil {
		panic(err)
	}

	fmt.Println(count)
	// Output: 3
}

func ExampleDB_Model_nullEmptyValue() {
	type Example struct {
		Hello string `sql:",null"`
	}

	var str sql.NullString
	_, err := db.QueryOne(pg.Scan(&str), "SELECT ?hello", Example{Hello: ""})
	if err != nil {
		panic(err)
	}
	fmt.Println(str.Valid)
	// Output: false
}

func ExampleDB_Model_hasOne() {
	db := modelDB()

	var book Book
	err := db.Model(&book).Column("book.*", "Author").First()
	if err != nil {
		panic(err)
	}
	fmt.Println(book, book.Author)
	// Output: Book<Id=1 Title="book 1"> Author<ID=1 Name="author 1">
}

func ExampleDB_Model_hasMany() {
	db := modelDB()

	var author Author
	err := db.Model(&author).Column("author.*", "Books").First()
	if err != nil {
		panic(err)
	}
	fmt.Println(author.Books[0], author.Books[1])
	// Output: Book<Id=1 Title="book 1"> Book<Id=2 Title="book 2">
}

func ExampleDB_Model_hasManyToMany() {
	db := modelDB()

	var book Book
	err := db.Model(&book).Column("book.*", "Genres").First()
	if err != nil {
		panic(err)
	}
	fmt.Println(book.Genres[0], book.Genres[1])
	// Output: Genre<Id=1 Name="genre 1"> Genre<Id=2 Name="genre 2">
}

func ExampleDB_Update() {
	db := modelDB()

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
	db := modelDB()

	book := Book{
		Id:       1,
		Title:    "updated book 1",
		AuthorID: 2, // this column will not be updated
	}
	_, err := db.Model(&book).Column("title").Returning("*").Update()
	if err != nil {
		panic(err)
	}

	fmt.Println(book, book.AuthorID)
	// Output: Book<Id=1 Title="updated book 1"> 1
}

func ExampleDB_Update_usingSqlFunction() {
	db := modelDB()

	id := 1
	data := map[string]interface{}{
		"title": pg.Q("concat(?, title, ?)", "prefix ", " suffix"),
	}
	var book Book
	_, err := db.Model(&book).
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
	db := modelDB()

	ids := pg.Ints{1, 2}
	data := map[string]interface{}{
		"title": pg.Q("concat(?, title, ?)", "prefix ", " suffix"),
	}

	var books []Book
	_, err := db.Model(&books).
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
	db := modelDB()

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

	err = db.Delete(book)
	fmt.Println(err)
	// Output: pg: no rows in result set
}

func ExampleDB_Delete_multipleRows() {
	db := modelDB()

	ids := pg.Ints{1, 2, 3}
	res, err := db.Model(Book{}).Where("id IN (?)", ids).Delete()
	if err != nil {
		panic(err)
	}
	fmt.Println("deleted", res.Affected())

	count, err := db.Model(Book{}).Count()
	if err != nil {
		panic(err)
	}
	fmt.Println("left", count)

	// Output: deleted 3
	// left 0
}

func ExampleQ() {
	db := modelDB()

	var maxId int
	err := db.Model(Book{}).Column(pg.Q("max(id)")).Select(&maxId)
	if err != nil {
		panic(err)
	}
	fmt.Println(maxId)
	// Output: 3
}

func ExampleF() {
	db := modelDB()

	var book Book
	err := db.Model(&book).Where("? = ?", pg.F("id"), 1).Select()
	if err != nil {
		panic(err)
	}
	fmt.Println(book)
	// Output: Book<Id=1 Title="book 1">
}
