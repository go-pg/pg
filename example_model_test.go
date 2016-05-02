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

	// For CountEstimate.
	_, err = db.Exec("VACUUM")
	if err != nil {
		panic(err)
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

func ExampleDB_Create_onConflictDoNothing() {
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

func ExampleDB_Create_onConflictDoUpdate() {
	db := modelDB()

	var book *Book
	for i := 0; i < 2; i++ {
		book = &Book{
			Id:    100,
			Title: fmt.Sprintf("title version #%d", i),
		}
		_, err := db.Model(book).
			OnConflict("(id) DO UPDATE").
			Set("title = ?title").
			Create()
		if err != nil {
			panic(err)
		}

		err = db.Select(book)
		if err != nil {
			panic(err)
		}
		fmt.Println(book)
	}

	err := db.Delete(book)
	if err != nil {
		panic(err)
	}

	// Output: Book<Id=100 Title="title version #0">
	// Book<Id=100 Title="title version #1">
}

func ExampleDB_Create_selectOrCreate() {
	db := modelDB()

	author := Author{
		Name: "R. Scott Bakker",
	}
	created, err := db.Model(&author).
		Column("id").
		Where("name = ?name").
		OnConflict("DO NOTHING"). // OnConflict is optional
		Returning("id").
		SelectOrCreate()
	if err != nil {
		panic(err)
	}
	fmt.Println(created, author)
	// Output: true Author<ID=2 Name="R. Scott Bakker">
}

func ExampleDB_Select() {
	db := modelDB()

	book := Book{
		Id: 1,
	}
	err := db.Select(&book)
	if err != nil {
		panic(err)
	}
	fmt.Println(book)
	// Output: Book<Id=1 Title="book 1">
}

func ExampleDB_Select_firstRow() {
	db := modelDB()

	var firstBook Book
	err := db.Model(&firstBook).First()
	if err != nil {
		panic(err)
	}
	fmt.Println(firstBook)
	// Output: Book<Id=1 Title="book 1">
}

func ExampleDB_Select_lastRow() {
	db := modelDB()

	var lastBook Book
	err := db.Model(&lastBook).Last()
	if err != nil {
		panic(err)
	}
	fmt.Println(lastBook)
	// Output: Book<Id=3 Title="book 3">
}

func ExampleDB_Select_allColumns() {
	db := modelDB()

	var book Book
	err := db.Model(&book).Column("book.*").First()
	if err != nil {
		panic(err)
	}
	fmt.Println(book, book.AuthorID)
	// Output: Book<Id=1 Title="book 1"> 1
}

func ExampleDB_Select_someColumns() {
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

func ExampleDB_Select_someColumnsIntoVars() {
	db := modelDB()

	var id int
	var title string
	err := db.Model(&Book{}).
		Column("book.id", "book.title").
		Order("book.id ASC").
		Limit(1).
		Select(&id, &title)
	if err != nil {
		panic(err)
	}

	fmt.Println(id, title)
	// Output: 1 book 1
}

func ExampleDB_Select_sqlExpression() {
	db := modelDB()

	var ids []int
	err := db.Model(&Book{}).
		ColumnExpr("array_agg(id)").
		Select(pg.Array(&ids))
	if err != nil {
		panic(err)
	}
	fmt.Println(ids)
	// Output: [1 2 3]
}

func ExampleDB_Select_groupBy() {
	db := modelDB()

	var res []struct {
		AuthorId  int
		BookCount int
	}

	err := db.Model(&Book{}).
		Column("author_id").
		ColumnExpr("count(*) AS book_count").
		Group("author_id").
		Order("book_count DESC").
		Select(&res)
	if err != nil {
		panic(err)
	}
	fmt.Println("len", len(res))
	fmt.Printf("author %d has %d books\n", res[0].AuthorId, res[0].BookCount)
	fmt.Printf("author %d has %d books\n", res[1].AuthorId, res[1].BookCount)
	// Output: len 2
	// author 1 has 2 books
	// author 11 has 1 books
}

func ExampleDB_Model_count() {
	db := modelDB()

	count, err := db.Model(&Book{}).Count()
	if err != nil {
		panic(err)
	}

	fmt.Println(count)
	// Output: 3
}

func ExampleDB_Model_countEstimate() {
	db := modelDB()

	count, err := db.Model(&Book{}).CountEstimate(0)
	if err != nil {
		panic(err)
	}

	fmt.Println(count)
	// Output: 3
}

func ExampleDB_Model_selectAndCount() {
	db := modelDB()

	var books []Book
	count, err := db.Model(&books).Order("id ASC").Limit(2).SelectAndCount()
	if err != nil {
		panic(err)
	}

	fmt.Println(count)
	fmt.Println(books)
	// Output: 3
	// [Book<Id=1 Title="book 1"> Book<Id=2 Title="book 2">]
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
	type Item struct {
		Id int

		Sub   *Item
		SubId int
	}

	db := connect()
	defer db.Close()

	qs := []string{
		"CREATE TEMP TABLE items (id int, sub_id int)",
		"INSERT INTO items VALUES (1, NULL), (2, 1), (3, NULL), (4, 2)",
	}
	for _, q := range qs {
		_, err := db.Exec(q)
		if err != nil {
			panic(err)
		}
	}

	// Select items and join subitem using following query:
	//
	// SELECT "item".*, "sub"."id" AS "sub__id", "sub"."sub_id" AS "sub__sub_id"
	// FROM "items" AS "item"
	// LEFT JOIN "items" AS "sub" ON "sub"."id" = item."sub_id"
	// WHERE (item.sub_id IS NOT NULL)

	var items []Item
	err := db.Model(&items).
		Column("item.*", "Sub").
		Where("item.sub_id IS NOT NULL").
		Select()
	if err != nil {
		panic(err)
	}

	fmt.Printf("found %d items\n", len(items))
	fmt.Printf("item %d, subitem %d\n", items[0].Id, items[0].Sub.Id)
	fmt.Printf("item %d, subitem %d\n", items[1].Id, items[1].Sub.Id)
	// Output: found 2 items
	// item 2, subitem 1
	// item 4, subitem 2
}

func ExampleDB_Model_hasMany() {
	type Item struct {
		Id       int
		Items    []Item `pg:",fk:Parent"`
		ParentId int
	}

	db := connect()
	defer db.Close()

	qs := []string{
		"CREATE TEMP TABLE items (id int, parent_id int)",
		"INSERT INTO items VALUES (1, NULL), (2, 1), (3, 1)",
	}
	for _, q := range qs {
		_, err := db.Exec(q)
		if err != nil {
			panic(err)
		}
	}

	// Select item and all subitems using following queries:
	//
	// SELECT "item".* FROM "items" AS "item" ORDER BY "item"."id" LIMIT 1
	//
	// SELECT "item".* FROM "items" AS "item" WHERE (("item"."parent_id") IN ((1)))

	var item Item
	err := db.Model(&item).Column("item.*", "Items").First()
	if err != nil {
		panic(err)
	}
	fmt.Println("Item", item.Id)
	fmt.Println("Subitems", item.Items[0].Id, item.Items[1].Id)
	// Output: Item 1
	// Subitems 2 3
}

func ExampleDB_Model_manyToMany() {
	type Item struct {
		Id    int
		Items []Item `pg:",many2many:item_to_items,joinFK:Sub"`
	}

	db := connect()
	defer db.Close()

	qs := []string{
		"CREATE TEMP TABLE items (id int)",
		"CREATE TEMP TABLE item_to_items (item_id int, sub_id int)",
		"INSERT INTO items VALUES (1), (2), (3)",
		"INSERT INTO item_to_items VALUES (1, 2), (1, 3)",
	}
	for _, q := range qs {
		_, err := db.Exec(q)
		if err != nil {
			panic(err)
		}
	}

	// Select item and all subitems using following queries:
	//
	// SELECT "item".* FROM "items" AS "item" ORDER BY "item"."id" LIMIT 1
	//
	// SELECT * FROM "items" AS "item"
	// JOIN "item_to_items" ON ("item_to_items"."item_id") IN ((1))
	// WHERE ("item"."id" = "item_to_items"."sub_id")

	var item Item
	err := db.Model(&item).Column("item.*", "Items").First()
	if err != nil {
		panic(err)
	}
	fmt.Println("Item", item.Id)
	fmt.Println("Subitems", item.Items[0].Id, item.Items[1].Id)
	// Output: Item 1
	// Subitems 2 3
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

func ExampleDB_Update_someColumns2() {
	db := modelDB()

	book := Book{
		Id:       1,
		Title:    "updated book 1",
		AuthorID: 2, // this column will not be updated
	}
	_, err := db.Model(&book).Set("title = ?title").Returning("*").Update()
	if err != nil {
		panic(err)
	}

	fmt.Println(book, book.AuthorID)
	// Output: Book<Id=1 Title="updated book 1"> 1
}

func ExampleDB_Update_setValues() {
	db := modelDB()

	var book Book
	_, err := db.Model(&book).
		Set("title = concat(?, title, ?)", "prefix ", " suffix").
		Where("id = ?", 1).
		Returning("*").
		Update()
	if err != nil {
		panic(err)
	}

	fmt.Println(book)
	// Output: Book<Id=1 Title="prefix book 1 suffix">
}

func ExampleDB_Update_updateValues() {
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

	fmt.Println(books)
	// Output: [Book<Id=1 Title="prefix book 1 suffix"> Book<Id=2 Title="prefix book 2 suffix">]
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
	res, err := db.Model(&Book{}).Where("id IN (?)", ids).Delete()
	if err != nil {
		panic(err)
	}
	fmt.Println("deleted", res.Affected())

	count, err := db.Model(&Book{}).Count()
	if err != nil {
		panic(err)
	}
	fmt.Println("left", count)

	// Output: deleted 3
	// left 0
}

func ExampleQ() {
	db := modelDB()

	cond := fmt.Sprintf("id = %d", 1)

	var book Book
	err := db.Model(&book).Where("?", pg.Q(cond)).Select()
	if err != nil {
		panic(err)
	}
	fmt.Println(book)
	// Output: Book<Id=1 Title="book 1">
}

func ExampleF() {
	db := modelDB()

	var book Book
	err := db.Model(&book).Where("? = 1", pg.F("id")).Select()
	if err != nil {
		panic(err)
	}
	fmt.Println(book)
	// Output: Book<Id=1 Title="book 1">
}
