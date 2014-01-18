package pg_test

import (
	"fmt"

	"github.com/vmihailenco/pg"
)

type ArticleFilter struct {
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

func CreateArticle(db *pg.DB, article *Article) error {
	_, err := db.ExecOne(`INSERT INTO articles VALUES (?name, ?category_id)`, article)
	return err
}

func GetArticles(db *pg.DB, f *ArticleFilter) ([]*Article, error) {
	articles := &Articles{}
	_, err := db.Query(articles, `
        SELECT * FROM articles WHERE 1=1 ?FilterName ?FilterCategory
    `, f)
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

	_, err := db.Exec(`CREATE TEMP TABLE articles (name text, category_id int)`)
	if err != nil {
		panic(err)
	}

	err = CreateArticle(db, &Article{"article1", 1})
	if err != nil {
		panic(err)
	}

	err = CreateArticle(db, &Article{"article2", 2})
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

	// Output: 2 &{article1 1} &{article2 2}
	// 1 &{article1 1}
}
