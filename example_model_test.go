package pg_test

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/go-pg/pg/v10"
	"github.com/go-pg/pg/v10/orm"
)

func modelDB() *pg.DB {
	db := pg.Connect(&pg.Options{
		User:     "postgres",
		Password: "postgres",
	})

	err := createTestSchema(db)
	if err != nil {
		panic(err)
	}

	_, err = db.Model(&Author{
		Name: "author 1",
	}).Insert()
	if err != nil {
		panic(err)
	}

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
	_, err = db.Model(&books).Insert()
	if err != nil {
		panic(err)
	}

	for i := 0; i < 2; i++ {
		genre := Genre{
			Name: fmt.Sprintf("genre %d", i+1),
		}
		_, err = db.Model(&genre).Insert()
		if err != nil {
			panic(err)
		}

		_, err = db.Model(&BookGenre{
			BookID:  1,
			GenreID: genre.ID,
		}).Insert()
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

func ExampleDB_Model_insert() {
	db := modelDB()

	book := &Book{
		Title:    "new book",
		AuthorID: 1,
	}

	_, err := db.Model(book).Insert()
	if err != nil {
		panic(err)
	}
	fmt.Println(book)
	// Output: Book<Id=4 Title="new book">

	_, err = db.Model(book).WherePK().Delete()
	if err != nil {
		panic(err)
	}
}

func ExampleDB_Model_bulkInsert() {
	db := modelDB()

	book1 := &Book{
		Title: "new book 1",
	}
	book2 := &Book{
		Title: "new book 2",
	}
	_, err := db.Model(book1, book2).Insert()
	if err != nil {
		panic(err)
	}
	fmt.Println(book1, book2)
	// Output: Book<Id=4 Title="new book 1"> Book<Id=5 Title="new book 2">

	for _, book := range []*Book{book1, book2} {
		_, err := db.Model(book).WherePK().Delete()
		if err != nil {
			panic(err)
		}
	}
}

func ExampleDB_Model_bulkInsertSlice() {
	db := modelDB()

	books := []Book{{
		Title: "new book 1",
	}, {
		Title: "new book 2",
	}}
	_, err := db.Model(&books).Insert()
	if err != nil {
		panic(err)
	}
	fmt.Println(books)
	// Output: [Book<Id=4 Title="new book 1"> Book<Id=5 Title="new book 2">]

	for i := range books {
		_, err := db.Model(&books[i]).WherePK().Delete()
		if err != nil {
			panic(err)
		}
	}
}

func ExampleDB_Model_insertOnConflictDoNothing() {
	db := modelDB()

	book := &Book{
		ID:    100,
		Title: "book 100",
	}

	for i := 0; i < 2; i++ {
		res, err := db.Model(book).OnConflict("DO NOTHING").Insert()
		if err != nil {
			panic(err)
		}
		if res.RowsAffected() > 0 {
			fmt.Println("created")
		} else {
			fmt.Println("did nothing")
		}
	}

	_, err := db.Model(book).WherePK().Delete()
	if err != nil {
		panic(err)
	}

	// Output: created
	// did nothing
}

func ExampleDB_Model_insertOnConflictDoUpdate() {
	db := modelDB()

	var book *Book
	for i := 0; i < 2; i++ {
		book = &Book{
			ID:    100,
			Title: fmt.Sprintf("title version #%d", i),
		}
		_, err := db.Model(book).
			OnConflict("(id) DO UPDATE").
			Set("title = EXCLUDED.title").
			Insert()
		if err != nil {
			panic(err)
		}

		err = db.Model(book).WherePK().Select()
		if err != nil {
			panic(err)
		}
		fmt.Println(book)
	}

	_, err := db.Model(book).WherePK().Delete()
	if err != nil {
		panic(err)
	}

	// Output: Book<Id=100 Title="title version #0">
	// Book<Id=100 Title="title version #1">
}

func ExampleDB_Model_selectOrInsert() {
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

func ExampleDB_Model_insertDynamicTableName() {
	type NamelessModel struct {
		tableName struct{} `pg:"_"` // "_" means no name
		Id        int
	}

	db := modelDB()

	err := db.Model((*NamelessModel)(nil)).Table("dynamic_name").CreateTable(nil)
	panicIf(err)

	row123 := &NamelessModel{
		Id: 123,
	}
	_, err = db.Model(row123).Table("dynamic_name").Insert()
	panicIf(err)

	row := new(NamelessModel)
	err = db.Model(row).Table("dynamic_name").First()
	panicIf(err)
	fmt.Println("id is", row.Id)

	err = db.Model((*NamelessModel)(nil)).Table("dynamic_name").DropTable(nil)
	panicIf(err)

	// Output: id is 123
}

func ExampleDB_Model_select() {
	db := modelDB()

	book := &Book{
		ID: 1,
	}
	err := db.Model(book).WherePK().Select()
	if err != nil {
		panic(err)
	}
	fmt.Println(book)
	// Output: Book<Id=1 Title="book 1">
}

func ExampleDB_Model_selectFirstRow() {
	db := modelDB()

	var firstBook Book
	err := db.Model(&firstBook).First()
	if err != nil {
		panic(err)
	}
	fmt.Println(firstBook)
	// Output: Book<Id=1 Title="book 1">
}

func ExampleDB_Model_selectLastRow() {
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

func ExampleDB_Model_selectSomeColumnsIntoVars() {
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

func ExampleDB_Model_selectWhereIn() {
	db := modelDB()

	var books []Book
	err := db.Model(&books).WhereIn("id IN (?)", []int{1, 2}).Select()
	if err != nil {
		panic(err)
	}
	fmt.Println(books)
	// Output: [Book<Id=1 Title="book 1"> Book<Id=2 Title="book 2">]
}

func ExampleDB_Model_selectWhereGroup() {
	db := modelDB()

	var books []Book
	err := db.Model(&books).
		WhereGroup(func(q *pg.Query) (*pg.Query, error) {
			q = q.WhereOr("id = 1").
				WhereOr("id = 2")
			return q, nil
		}).
		Where("title IS NOT NULL").
		Select()
	if err != nil {
		panic(err)
	}
	fmt.Println(books)
	// Output: [Book<Id=1 Title="book 1"> Book<Id=2 Title="book 2">]
}

func ExampleDB_Model_selectSQLExpression() {
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

func ExampleDB_Model_selectGroupBy() {
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

func ExampleDB_Model_selectWith() {
	authorBooks := pgdb.Model(&Book{}).Where("author_id = ?", 1)

	var books []Book
	err := pgdb.Model().
		With("author_books", authorBooks).
		Table("author_books").
		Select(&books)
	if err != nil {
		panic(err)
	}
	fmt.Println(books)
	// Output: [Book<Id=1 Title="book 1"> Book<Id=2 Title="book 2">]
}

func ExampleDB_Model_selectWrapWith() {
	// WITH author_books AS (
	//     SELECT * books WHERE author_id = 1
	// )
	// SELECT * FROM author_books
	var books []Book
	err := pgdb.Model(&books).
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

func ExampleDB_Model_selectApplyFunc() {
	db := modelDB()

	var authorId int
	var editorId int

	filter := func(q *pg.Query) (*pg.Query, error) {
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

func ExampleDB_Model_exists() {
	db := modelDB()

	var books []Book
	exists, err := db.Model(&books).Where("author_id = ?", 1).Exists()
	if err != nil {
		panic(err)
	}

	fmt.Println(exists)
	// Output: true
}

func ExampleDB_Model_nullEmptyValue() {
	type Example struct {
		Hello string
	}

	var str sql.NullString
	_, err := pgdb.QueryOne(pg.Scan(&str), "SELECT ?hello", &Example{Hello: ""})
	if err != nil {
		panic(err)
	}
	fmt.Println(str.Valid)
	// Output: false
}

func ExampleDB_Model_forEach() {
	err := pgdb.Model((*Book)(nil)).
		OrderExpr("id ASC").
		ForEach(func(b *Book) error {
			fmt.Println(b)
			return nil
		})
	if err != nil {
		panic(err)
	}
	// Output: Book<Id=1 Title="book 1">
	// Book<Id=2 Title="book 2">
	// Book<Id=3 Title="book 3">
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
		Profile   *Profile `pg:"rel:has-one"`
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
	//   "profile"."lang" AS "profile__lang"
	// FROM "users" AS "user"
	// LEFT JOIN "profiles" AS "profile" ON "profile"."id" = "user"."profile_id"

	var users []User
	err := db.Model(&users).
		Column("user.*").
		Relation("Profile").
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
		Profile *Profile `pg:"rel:belongs-to"`
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
	//   "profile"."lang" AS "profile__lang",
	//   "profile"."user_id" AS "profile__user_id"
	// FROM "users" AS "user"
	// LEFT JOIN "profiles" AS "profile" ON "profile"."user_id" = "user"."id"

	var users []User
	err := db.Model(&users).
		Column("user.*").
		Relation("Profile").
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
		Profiles []*Profile `pg:"rel:has-many"`
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
		Column("user.*").
		Relation("Profiles", func(q *pg.Query) (*pg.Query, error) {
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
		Items    []Item `pg:"rel:has-many,join_fk:parent_id"`
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
	err := db.Model(&item).Column("item.*").Relation("Items").First()
	if err != nil {
		panic(err)
	}
	fmt.Println("Item", item.Id)
	fmt.Println("Subitems", item.Items[0].Id, item.Items[1].Id)
	// Output: Item 1
	// Subitems 2 3
}

func ExampleDB_Model_update() {
	db := modelDB()

	book := &Book{ID: 1}
	err := db.Model(book).WherePK().Select()
	if err != nil {
		panic(err)
	}

	book.Title = "updated book 1"
	_, err = db.Model(book).WherePK().Update()
	if err != nil {
		panic(err)
	}

	err = db.Model(book).WherePK().Select()
	if err != nil {
		panic(err)
	}

	fmt.Println(book)
	// Output: Book<Id=1 Title="updated book 1">
}

func ExampleDB_Model_updateNotZero() {
	db := modelDB()

	book := &Book{
		ID:    1,
		Title: "updated book 1",
	}
	_, err := db.Model(book).WherePK().UpdateNotZero()
	if err != nil {
		panic(err)
	}

	book = new(Book)
	err = db.Model(book).Where("id = ?", 1).Select()
	if err != nil {
		panic(err)
	}

	fmt.Println(book)
	// Output: Book<Id=1 Title="updated book 1">
}

func ExampleDB_Model_updateUseZeroBool() {
	type Event struct {
		ID     int
		Active bool `pg:",use_zero"`
	}

	db := pg.Connect(pgOptions())
	defer db.Close()

	err := db.Model((*Event)(nil)).CreateTable(&orm.CreateTableOptions{
		Temp: true,
	})
	if err != nil {
		panic(err)
	}

	event := &Event{
		ID:     1,
		Active: true,
	}
	_, err = db.Model(event).Insert()
	if err != nil {
		panic(err)
	}

	fmt.Println(event)

	event.Active = false
	_, err = db.Model(event).WherePK().UpdateNotZero()
	if err != nil {
		panic(err)
	}

	event2 := new(Event)
	err = db.Model(event2).Where("id = ?", 1).Select()
	if err != nil {
		panic(err)
	}

	fmt.Println(event2)
	// Output: &{1 true}
	// &{1 false}
}

func ExampleDB_Model_updateSomeColumns() {
	db := modelDB()

	book := Book{
		ID:       1,
		Title:    "updated book 1", // only this column is going to be updated
		AuthorID: 2,
	}
	_, err := db.Model(&book).Column("title").WherePK().Returning("*").Update()
	if err != nil {
		panic(err)
	}

	fmt.Println(book, book.AuthorID)
	// Output: Book<Id=1 Title="updated book 1"> 1
}

func ExampleDB_Model_updateSomeColumns2() {
	db := modelDB()

	book := Book{
		ID:       1,
		Title:    "updated book 1",
		AuthorID: 2, // this column will not be updated
	}
	_, err := db.Model(&book).Set("title = ?title").WherePK().Returning("*").Update()
	if err != nil {
		panic(err)
	}

	fmt.Println(book, book.AuthorID)
	// Output: Book<Id=1 Title="updated book 1"> 1
}

func ExampleDB_Model_updateSetValues() {
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

func ExampleDB_Model_bulkUpdate() {
	db := modelDB()

	book1 := &Book{
		ID:        1,
		Title:     "updated book 1",
		UpdatedAt: time.Now(),
	}
	book2 := &Book{
		ID:        2,
		Title:     "updated book 2",
		UpdatedAt: time.Now(),
	}

	// UPDATE "books" AS "book"
	// SET "title" = _data."title"
	// FROM (VALUES ('updated book 1', 1), ('updated book 2', 2)) AS _data("title", "id")
	// WHERE "book"."id" = _data."id"
	_, err := db.Model(book1, book2).Column("title", "updated_at").Update()
	if err != nil {
		panic(err)
	}

	var books []Book
	err = db.Model(&books).Order("id").Select()
	if err != nil {
		panic(err)
	}

	fmt.Println(books)
	// Output: [Book<Id=1 Title="updated book 1"> Book<Id=2 Title="updated book 2"> Book<Id=3 Title="book 3">]
}

func ExampleDB_Model_bulkUpdateSlice() {
	db := modelDB()

	books := []Book{{
		ID:        1,
		Title:     "updated book 1",
		UpdatedAt: time.Now(),
	}, {
		ID:        2,
		Title:     "updated book 2",
		UpdatedAt: time.Now(),
	}}

	// UPDATE "books" AS "book"
	// SET "title" = _data."title"
	// FROM (VALUES ('updated book 1', 1), ('updated book 2', 2)) AS _data("title", "id")
	// WHERE "book"."id" = _data."id"
	_, err := db.Model(&books).Column("title", "updated_at").Update()
	if err != nil {
		panic(err)
	}

	books = nil
	err = db.Model(&books).Order("id").Select()
	if err != nil {
		panic(err)
	}

	fmt.Println(books)
	// Output: [Book<Id=1 Title="updated book 1"> Book<Id=2 Title="updated book 2"> Book<Id=3 Title="book 3">]
}

func ExampleDB_Model_delete() {
	db := modelDB()

	book := &Book{
		Title:    "title 1",
		AuthorID: 1,
	}
	_, err := db.Model(book).Insert()
	if err != nil {
		panic(err)
	}

	_, err = db.Model(book).WherePK().Delete()
	if err != nil {
		panic(err)
	}

	err = db.Model(book).WherePK().Select()
	fmt.Println(err)
	// Output: pg: no rows in result set
}

func ExampleDB_Model_deleteMultipleRows() {
	db := modelDB()

	ids := pg.In([]int{1, 2, 3})
	res, err := db.Model((*Book)(nil)).Where("id IN (?)", ids).Delete()
	if err != nil {
		panic(err)
	}
	fmt.Println("deleted", res.RowsAffected())

	count, err := db.Model((*Book)(nil)).Count()
	if err != nil {
		panic(err)
	}
	fmt.Println("left", count)

	// Output: deleted 3
	// left 0
}

func ExampleDB_Model_bulkDelete() {
	db := modelDB()

	var books []Book
	err := db.Model(&books).Select()
	if err != nil {
		panic(err)
	}

	res, err := db.Model(&books).Delete()
	if err != nil {
		panic(err)
	}
	fmt.Println("deleted", res.RowsAffected())

	count, err := db.Model((*Book)(nil)).Count()
	if err != nil {
		panic(err)
	}
	fmt.Println("left", count)

	// Output: deleted 3
	// left 0
}

func ExampleSafe() {
	db := modelDB()

	cond := fmt.Sprintf("id = %d", 1)

	var book Book
	err := db.Model(&book).Where("?", pg.Safe(cond)).Select()
	if err != nil {
		panic(err)
	}
	fmt.Println(book)
	// Output: Book<Id=1 Title="book 1">
}

func ExampleIdent() {
	db := modelDB()

	var book Book
	err := db.Model(&book).Where("? = 1", pg.Ident("id")).Select()
	if err != nil {
		panic(err)
	}
	fmt.Println(book)
	// Output: Book<Id=1 Title="book 1">
}

func ExampleDB_jsonUseNumber() {
	type Event struct {
		Id   int
		Data map[string]interface{} `pg:",json_use_number"`
	}

	db := pg.Connect(pgOptions())
	defer db.Close()

	err := db.Model((*Event)(nil)).CreateTable(&orm.CreateTableOptions{
		Temp: true,
	})
	if err != nil {
		panic(err)
	}

	event := &Event{
		Data: map[string]interface{}{
			"price": 1.23,
		},
	}
	_, err = db.Model(event).Insert()
	if err != nil {
		panic(err)
	}

	event2 := new(Event)
	err = db.Model(event2).Where("id = ?", event.Id).Select()
	if err != nil {
		panic(err)
	}

	// Check that price is decoded as json.Number.
	fmt.Printf("%T", event2.Data["price"])
	// Output: json.Number
}

func ExampleDB_Model_discardUnknownColumns() {
	type Model1 struct {
	}

	var model1 Model1
	_, err := pgdb.QueryOne(&model1, "SELECT 1 AS id")
	fmt.Printf("Model1: %v\n", err)

	type Model2 struct {
		tableName struct{} `pg:",discard_unknown_columns"`
	}

	var model2 Model2
	_, err = pgdb.QueryOne(&model2, "SELECT 1 AS id")
	fmt.Printf("Model2: %v\n", err)

	// Output: Model1: pg: can't find column=id in model=Model1 (prefix the column with underscore or use discard_unknown_columns)
	// Model2: <nil>
}

func ExampleDB_Model_softDelete() {
	type Flight struct {
		Id        int
		Name      string
		DeletedAt time.Time `pg:",soft_delete"`
	}

	err := pgdb.Model((*Flight)(nil)).CreateTable(&orm.CreateTableOptions{
		Temp: true,
	})
	panicIf(err)

	flight1 := &Flight{
		Id: 1,
	}
	_, err = pgdb.Model(flight1).Insert()
	panicIf(err)

	// Soft delete.
	_, err = pgdb.Model(flight1).WherePK().Delete()
	panicIf(err)

	// Count visible flights.
	count, err := pgdb.Model((*Flight)(nil)).Count()
	panicIf(err)
	fmt.Println("count", count)

	// Count soft deleted flights.
	deletedCount, err := pgdb.Model((*Flight)(nil)).Deleted().Count()
	panicIf(err)
	fmt.Println("deleted count", deletedCount)

	// Actually delete the flight.
	_, err = pgdb.Model(flight1).WherePK().ForceDelete()
	panicIf(err)

	// Count soft deleted flights.
	deletedCount, err = pgdb.Model((*Flight)(nil)).Deleted().Count()
	panicIf(err)
	fmt.Println("deleted count", deletedCount)

	// Output: count 0
	// deleted count 1
	// deleted count 0
}
