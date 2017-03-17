package orm

import (
	"net/http"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type URLValuesModel struct {
	Id   int
	Name string
}

type urlValuesTest struct {
	url   string
	query string
}

var _ = Describe("URLValues", func() {
	query := `SELECT "url_values_model"."id", "url_values_model"."name" FROM "url_values_models" AS "url_values_model"`
	urlValuesTests := []urlValuesTest{
		{
			url:   "http://localhost:8000/test?id__gt=1",
			query: query + ` WHERE ("id" > '1')`,
		},
		{
			url:   "http://localhost:8000/test?name__gte=Michael",
			query: query + ` WHERE ("name" >= 'Michael')`,
		},
		{
			url:   "http://localhost:8000/test?id__lt=10",
			query: query + ` WHERE ("id" < '10')`,
		},
		{
			url:   "http://localhost:8000/test?name__lte=Peter",
			query: query + ` WHERE ("name" <= 'Peter')`,
		},
		{
			url:   "http://localhost:8000/test?name__exclude=Peter",
			query: query + ` WHERE ("name" != 'Peter')`,
		},
		{
			url:   "http://localhost:8000/test?name__exclude=Mike&name__exclude=Peter",
			query: query + ` WHERE ("name" NOT IN ('Mike','Peter'))`,
		},
		{
			url:   "http://localhost:8000/test?name=Mike",
			query: query + ` WHERE ("name" = 'Mike')`,
		},
		{
			url:   "http://localhost:8000/test?name__ieq=mik_",
			query: query + ` WHERE ("name" ILIKE 'mik_')`,
		},
		{
			url:   "http://localhost:8000/test?name__match=(m|p).*",
			query: query + ` WHERE ("name" SIMILAR TO '(m|p).*')`,
		},
		{
			url:   "http://localhost:8000/test?name__include=Peter&name__include=Mike",
			query: query + ` WHERE ("name" IN ('Peter','Mike'))`,
		},
		{
			url:   "http://localhost:8000/test?name=Mike&name=Peter",
			query: query + ` WHERE ("name" IN ('Mike','Peter'))`,
		},
		{
			url:   "http://localhost:8000/test?invalid_field=1",
			query: query,
		},
	}

	It("adds conditions to the query", func() {
		for _, urlValuesTest := range urlValuesTests {
			req, _ := http.NewRequest("GET", urlValuesTest.url, nil)

			q := NewQuery(nil, &URLValuesModel{})
			q = q.Apply(URLFilters(req.URL.Query()))

			b, err := selectQuery{Query: q}.AppendQuery(nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(b)).To(Equal(urlValuesTest.query))
		}
	})
})

var _ = Describe("Pager", func() {
	query := `SELECT "url_values_model"."id", "url_values_model"."name" FROM "url_values_models" AS "url_values_model"`
	urlValuesTests := []urlValuesTest{
		{
			url:   "http://localhost:8000/test?limit=10",
			query: query + " LIMIT 10",
		},
		{
			url:   "http://localhost:8000/test?page=5",
			query: query + ` LIMIT 100 OFFSET 400`,
		},

		{
			url:   "http://localhost:8000/test?page=5&limit=20",
			query: query + ` LIMIT 20 OFFSET 80`,
		},
	}

	It("adds limit and offset to the query", func() {
		for _, urlValuesTest := range urlValuesTests {
			req, _ := http.NewRequest("GET", urlValuesTest.url, nil)

			q := NewQuery(nil, &URLValuesModel{})
			q = q.Apply(Pagination(req.URL.Query(), 100))

			b, err := selectQuery{Query: q}.AppendQuery(nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(b)).To(Equal(urlValuesTest.query))
		}
	})
})
