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

func (f *ArticleFilter) FilterName() pg.Q {
	if f.Name == "" {
		return ""
	}
	return pg.MustFormatQ("AND name = ?", f.Name)
}

func (f *ArticleFilter) FilterCategory() pg.Q {
	if f.CategoryId == 0 {
		return ""
	}
	return pg.MustFormatQ("AND category_id = ?", f.CategoryId)
}

type Article struct {
	Id         int64
	Name       string
	CategoryId int
}

func CreateArticle(db *pg.DB, article *Article) error {
	_, err := db.ExecOne(`
		INSERT INTO articles (name, category_id)
		VALUES (?name, ?category_id)
	`, article)
	return err
}

func GetArticle(db *pg.DB, id int64) (*Article, error) {
	article := &Article{}
	_, err := db.QueryOne(article, `SELECT * FROM articles WHERE id = ?`, id)
	return article, err
}

func GetArticles(db *pg.DB, f *ArticleFilter) ([]Article, error) {
	var articles []Article
	_, err := db.Query(&articles, `
		SELECT * FROM articles WHERE 1=1 ?FilterName ?FilterCategory
	`, f)
	if err != nil {
		return nil, err
	}
	return articles, nil
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
