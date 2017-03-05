package pg_test

import (
	"database/sql"
	"fmt"
	"time"

	"gopkg.in/pg.v5"
	"gopkg.in/pg.v5/orm"
)

func modelDB() *pg.DB {
	db := pg.Connect(&pg.Options{
		User: "postgres",
	})

	err := createTestSchema(db)
	if err != nil {
		panic(err)
	}

	err = db.Insert(&Author{
		Name: "author 1",
	})

	books := []Book{{
		Title:    "book 1",
		AuthorID: 1,
		EditorID: 11,
	}, {
		Title:    "book 2",
		AuthorID: 1,
		EditorID: 12,
	}, {
		Title:     "book 3",
		AuthorID:  11,
		EditorID:  11,
		CreatedAt: time.Now(),
	}}
	err = db.Insert(&books)
	if err != nil {
		panic(err)
	}

	for i := 0; i < 2; i++ {
		genre := Genre{
			Name: fmt.Sprintf("genre %d", i+1),
		}
		err = db.Insert(&genre)
		if err != nil {
			panic(err)
		}

		err = db.Insert(&BookGenre{
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

func ExampleDB_Insert() {
	db := modelDB()

	book := Book{
		Title:    "new book",
		AuthorID: 1,
	}

	err := db.Insert(&book)
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

func ExampleDB_Insert_bulkInsert() {
	db := modelDB()

	book1 := Book{
		Title: "new book 1",
	}
	book2 := Book{
		Title: "new book 2",
	}
	err := db.Insert(&book1, &book2)
	if err != nil {
		panic(err)
	}
	fmt.Println(book1, book2)
	// Output: Book<Id=4 Title="new book 1"> Book<Id=5 Title="new book 2">

	for _, book := range []*Book{&book1, &book2} {
		err := db.Delete(book)
		if err != nil {
			panic(err)
		}
	}
}

func ExampleDB_Insert_bulkInsert2() {
	db := modelDB()

	books := []Book{{
		Title: "new book 1",
	}, {
		Title: "new book 2",
	}}
	err := db.Insert(&books)
	if err != nil {
		panic(err)
	}
	fmt.Println(books)
	// Output: [Book<Id=4 Title="new book 1"> Book<Id=5 Title="new book 2">]

	for i := range books {
		err := db.Delete(&books[i])
		if err != nil {
			panic(err)
		}
	}
}

func ExampleDB_Insert_onConflictDoNothing() {
	db := modelDB()

	book := Book{
		Id:    100,
		Title: "book 100",
	}

	for i := 0; i < 2; i++ {
		res, err := db.Model(&book).OnConflict("DO NOTHING").Insert()
		if err != nil {
			panic(err)
		}
		if res.RowsAffected() > 0 {
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

func ExampleDB_Insert_onConflictDoUpdate() {
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
			Insert()
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

func ExampleDB_Insert_selectOrInsert() {
	db := modelDB()

	author := Author{
		Name: "R. Scott Bakker",
	}
	created, err := db.Model(&author).
		Column("id").
		Where("name = ?name").
		OnConflict("DO NOTHING"). // OnConflict is optional
		Returning("id").
		SelectOrInsert()
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

func ExampleDB_Select_whereIn() {
	db := modelDB()

	var ids = []interface{}{}
	for _, author := range authors {
		ids = append(ids, author.ID)
	}

	var book []Book
	if err := db.Model(&book).WhereIn("author_id", ids...).Select(); err != nil {
		return nil, err
	}
	fmt.Println(book[0])
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
		Column("book.id", "book.title").
		OrderExpr("book.id ASC").
		Limit(1).
		Select()
	if err != nil {
		panic(err)
	}

	fmt.Println(book)
	// Output: Book<Id=1 Title="book 1">
}

func ExampleDB_Select_someColumnsIntoVars() {
	db := modelDB()

	var id int
	var title string
	err := db.Model(&Book{}).
		Column("book.id", "book.title").
		OrderExpr("book.id ASC").
		Limit(1).
		Select(&id, &title)
	if err != nil {
		panic(err)
	}

	fmt.Println(id, title)
	// Output: 1 book 1
}

func ExampleDB_Select_whereIn() {
	db := modelDB()

	var books []Book
	err := db.Model(&books).WhereIn("id IN (?)", 1, 2).Select()
	if err != nil {
		panic(err)
	}
	fmt.Println(books)
	// Output: [Book<Id=1 Title="book 1"> Book<Id=2 Title="book 2">]
}

func ExampleDB_Select_sqlExpression() {
	db := modelDB()

	var ids []int
	err := db.Model(&Book{}).
		ColumnExpr("array_agg(book.id)").
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
		OrderExpr("book_count DESC").
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

func ExampleDB_Select_with() {
	authorBooks := db.Model(&Book{}).Where("author_id = ?", 1)

	var books []Book
	err := db.Model().
		With("author_books", authorBooks).
		Table("author_books").
		Select(&books)
	if err != nil {
		panic(err)
	}
	fmt.Println(books)
	// Output: [Book<Id=1 Title="book 1"> Book<Id=2 Title="book 2">]
}

func ExampleDB_Select_wrapWith() {
	// WITH author_books AS (
	//     SELECT * books WHERE author_id = 1
	// )
	// SELECT * FROM author_books
	var books []Book
	err := db.Model(&books).
		Where("author_id = ?", 1).
		WrapWith("author_books").
		Table("author_books").
		Select(&books)
	if err != nil {
		panic(err)
	}
	fmt.Println(books)
	// Output: [Book<Id=1 Title="book 1"> Book<Id=2 Title="book 2">]
}

func ExampleDB_Select_applyFunc() {
	db := modelDB()

	var authorId int
	var editorId int

	filter := func(q *orm.Query) (*orm.Query, error) {
		if authorId != 0 {
			q = q.Where("author_id = ?", authorId)
		}
		if editorId != 0 {
			q = q.Where("editor_id = ?", editorId)
		}
		return q, nil
	}

	var books []Book
	authorId = 1
	err := db.Model(&books).
		Apply(filter).
		Select()
	if err != nil {
		panic(err)
	}
	fmt.Println(books)
	// Output: [Book<Id=1 Title="book 1"> Book<Id=2 Title="book 2">]
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
	count, err := db.Model(&books).OrderExpr("id ASC").Limit(2).SelectAndCount()
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
		Hello string
	}

	var str sql.NullString
	_, err := db.QueryOne(pg.Scan(&str), "SELECT ?hello", &Example{Hello: ""})
	if err != nil {
		panic(err)
	}
	fmt.Println(str.Valid)
	// Output: false
}

func ExampleDB_Model_hasOne() {
	type Profile struct {
		Id   int
		Lang string
	}

	// User has one profile.
	type User struct {
		Id        int
		Name      string
		ProfileId int
		Profile   *Profile
	}

	db := connect()
	defer db.Close()

	qs := []string{
		"CREATE TEMP TABLE users (id int, name text, profile_id int)",
		"CREATE TEMP TABLE profiles (id int, lang text)",
		"INSERT INTO users VALUES (1, 'user 1', 1), (2, 'user 2', 2)",
		"INSERT INTO profiles VALUES (1, 'en'), (2, 'ru')",
	}
	for _, q := range qs {
		_, err := db.Exec(q)
		if err != nil {
			panic(err)
		}
	}

	// Select users joining their profiles with following query:
	//
	// SELECT
	//   "user".*,
	//   "profile"."id" AS "profile__id",
	//   "profile"."lang" AS "profile__lang",
	//   "profile"."user_id" AS "profile__user_id"
	// FROM "users" AS "user"
	// LEFT JOIN "profiles" AS "profile" ON "profile"."user_id" = "user"."id"

	var users []User
	err := db.Model(&users).
		Column("user.*", "Profile").
		Select()
	if err != nil {
		panic(err)
	}

	fmt.Println(len(users), "results")
	fmt.Println(users[0].Id, users[0].Name, users[0].Profile)
	fmt.Println(users[1].Id, users[1].Name, users[1].Profile)
	// Output: 2 results
	// 1 user 1 &{1 en}
	// 2 user 2 &{2 ru}
}

func ExampleDB_Model_belongsTo() {
	// Profile belongs to User.
	type Profile struct {
		Id     int
		Lang   string
		UserId int
	}

	type User struct {
		Id      int
		Name    string
		Profile *Profile
	}

	db := connect()
	defer db.Close()

	qs := []string{
		"CREATE TEMP TABLE users (id int, name text)",
		"CREATE TEMP TABLE profiles (id int, lang text, user_id int)",
		"INSERT INTO users VALUES (1, 'user 1'), (2, 'user 2')",
		"INSERT INTO profiles VALUES (1, 'en', 1), (2, 'ru', 2)",
	}
	for _, q := range qs {
		_, err := db.Exec(q)
		if err != nil {
			panic(err)
		}
	}

	// Select users joining their profiles with following query:
	//
	// SELECT
	//   "user".*,
	//   "profile"."id" AS "profile__id",
	//   "profile"."lang" AS "profile__lang"
	// FROM "users" AS "user"
	// LEFT JOIN "profiles" AS "profile" ON "profile"."id" = "user"."profile_id"

	var users []User
	err := db.Model(&users).
		Column("user.*", "Profile").
		Select()
	if err != nil {
		panic(err)
	}

	fmt.Println(len(users), "results")
	fmt.Println(users[0].Id, users[0].Name, users[0].Profile)
	fmt.Println(users[1].Id, users[1].Name, users[1].Profile)
	// Output: 2 results
	// 1 user 1 &{1 en 1}
	// 2 user 2 &{2 ru 2}
}

func ExampleDB_Model_hasMany() {
	type Profile struct {
		Id     int
		Lang   string
		Active bool
		UserId int
	}

	// User has many profiles.
	type User struct {
		Id       int
		Name     string
		Profiles []*Profile
	}

	db := connect()
	defer db.Close()

	qs := []string{
		"CREATE TEMP TABLE users (id int, name text)",
		"CREATE TEMP TABLE profiles (id int, lang text, active bool, user_id int)",
		"INSERT INTO users VALUES (1, 'user 1')",
		"INSERT INTO profiles VALUES (1, 'en', TRUE, 1), (2, 'ru', TRUE, 1), (3, 'md', FALSE, 1)",
	}
	for _, q := range qs {
		_, err := db.Exec(q)
		if err != nil {
			panic(err)
		}
	}

	// Select user and all his active profiles with following queries:
	//
	// SELECT "user".* FROM "users" AS "user" ORDER BY "user"."id" LIMIT 1
	//
	// SELECT "profile".* FROM "profiles" AS "profile"
	// WHERE (active IS TRUE) AND (("profile"."user_id") IN ((1)))

	var user User
	err := db.Model(&user).
		Column("user.*", "Profiles").
		Relation("Profiles", func(q *orm.Query) (*orm.Query, error) {
			return q.Where("active IS TRUE"), nil
		}).
		First()
	if err != nil {
		panic(err)
	}
	fmt.Println(user.Id, user.Name, user.Profiles[0], user.Profiles[1])
	// Output: 1 user 1 &{1 en true 1} &{2 ru true 1}
}

func ExampleDB_Model_hasManySelf() {
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

	// Select item and all subitems with following queries:
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

	// Select item and all subitems with following queries:
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
		Title:    "updated book 1", // only this column is going to be updated
		AuthorID: 2,
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

func ExampleDB_Delete() {
	db := modelDB()

	book := Book{
		Title:    "title 1",
		AuthorID: 1,
	}
	err := db.Insert(&book)
	if err != nil {
		panic(err)
	}

	err = db.Delete(&book)
	if err != nil {
		panic(err)
	}

	err = db.Select(&book)
	fmt.Println(err)
	// Output: pg: no rows in result set
}

func ExampleDB_Delete_multipleRows() {
	db := modelDB()

	ids := pg.In([]int{1, 2, 3})
	res, err := db.Model(&Book{}).Where("id IN (?)", ids).Delete()
	if err != nil {
		panic(err)
	}
	fmt.Println("deleted", res.RowsAffected())

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
