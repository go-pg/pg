package pg_test

import (
	"fmt"

	"github.com/vmihailenco/pg"
)

type ArticleFilter struct {
	CategoryId int
}

func (f *ArticleFilter) CategoryClause() pg.Q {
	if f.CategoryId == 0 {
		return ""
	}
	return pg.MustFormatQ("AND category_id = ?", f.CategoryId)
}

type Article struct {
	Name       string
	CategoryId int
}

type Articles struct {
	Values []*Article
}

func (f *Articles) New() interface{} {
	a := &Article{}
	f.Values = append(f.Values, a)
	return a
}

func GetArticles(db *pg.DB, f *ArticleFilter) ([]*Article, error) {
	articles := &Articles{}
	_, err := db.Query(articles, `
        WITH articles (name, category_id) AS (VALUES (?, ?), (?, ?))
        SELECT * FROM articles WHERE 1=1 ?
    `, "article1", 1, "article2", 2, f.CategoryClause())
	if err != nil {
		return nil, err
	}
	return articles.Values, nil
}

func Example_complexQuery() {
	db := pg.Connect(&pg.Options{
		User: "postgres",
	})
	defer db.Close()

	articles, err := GetArticles(db, &ArticleFilter{})
	fmt.Println(articles[0], articles[1], err)

	articles, err = GetArticles(db, &ArticleFilter{CategoryId: 1})
	fmt.Println(articles[0], err)

	// Output: &{article1 1} &{article2 2} <nil>
	// &{article1 1} <nil>
}
