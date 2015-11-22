package pg_test

import (
	"fmt"

	"gopkg.in/pg.v3"
)

type ArticleFilter struct {
	Id         int64
	Name       string
	CategoryId int
}

func (f *ArticleFilter) NameFilter() pg.Q {
	if f.Name == "" {
		return ""
	}
	return pg.MustFormatQ("name = ?", f.Name)
}

func (f *ArticleFilter) CategoryFilter() pg.Q {
	if f.CategoryId == 0 {
		return ""
	}
	return pg.MustFormatQ("category_id = ?", f.CategoryId)
}

func (f *ArticleFilter) Filters() pg.Q {
	return pg.Where(f.NameFilter(), f.CategoryFilter())
}

type Article struct {
	Id         int64 `pg:",nullempty"`
	Name       string
	CategoryId int
}

func CreateArticle(db *pg.DB, article *Article) error {
	model := pg.NewModel(article, "")
	_, err := db.QueryOne(model, `
		INSERT INTO articles (?Fields) VALUES (?Values)
		RETURNING id
	`, model)
	return err
}

func GetArticle(db *pg.DB, id int64) (*Article, error) {
	var article Article
	_, err := db.QueryOne(&article, `SELECT * FROM articles WHERE id = ?`, id)
	return &article, err
}

func GetArticles(db *pg.DB, f *ArticleFilter) ([]Article, error) {
	var articles []Article
	_, err := db.Query(&articles, `
		SELECT * FROM articles WHERE ?Filters
	`, f)
	return articles, err
}

func Example_complexQuery() {
	db := pg.Connect(&pg.Options{
		User: "postgres",
	})
	defer db.Close()

	_, err := db.Exec(`CREATE TEMP TABLE articles (id serial, name text, category_id int)`)
	if err != nil {
		panic(err)
	}

	err = CreateArticle(db, &Article{Name: "article1", CategoryId: 1})
	if err != nil {
		panic(err)
	}

	err = CreateArticle(db, &Article{Name: "article2", CategoryId: 2})
	if err != nil {
		panic(err)
	}

	articles, err := GetArticles(db, &ArticleFilter{})
	if err != nil {
		panic(err)
	}
	fmt.Printf("%d %v %v\n", len(articles), articles[0], articles[1])

	articles, err = GetArticles(db, &ArticleFilter{CategoryId: 1})
	if err != nil {
		panic(err)
	}
	fmt.Printf("%d %v\n", len(articles), articles[0])

	// Output: 2 {1 article1 1} {2 article2 2}
	// 1 {1 article1 1}
}
