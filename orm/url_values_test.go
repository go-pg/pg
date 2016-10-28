package orm

import (
	"net/http"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type URLValuesTest struct {
	url   string
	query string
}

var _ = Describe("URLValues", func() {
	It("URLValues add conditions to query from request url parameters", func() {
		query := `SELECT "select_test"."id", "select_test"."name" FROM "select_tests" AS "select_test"`

		urlValuesTests := []URLValuesTest{
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
				url:   "http://localhost:8000/test?name__include=Peter&name__include=Mike",
				query: query + ` WHERE ("name" IN ('Peter','Mike'))`,
			},
			{
				url:   "http://localhost:8000/test?name=Mike&name=Peter",
				query: query + ` WHERE ("name" IN ('Mike','Peter'))`,
			},
			{
				url:   "http://localhost:8000/test?order=name DESC",
				query: query + ` ORDER BY name DESC`,
			},
			{
				url:   "http://localhost:8000/test?order=id ASC&order=name DESC",
				query: query + ` ORDER BY id ASC, name DESC`,
			},
			{
				url:   "http://localhost:8000/test?invalid_field=1",
				query: query,
			},
		}

		for _, urlValuesTest := range urlValuesTests {
			req, _ := http.NewRequest("GET", urlValuesTest.url, nil)

			q := NewQuery(nil, &SelectTest{})
			q = q.Apply(URLValues(req.URL.Query()))

			b, err := q.selectQuery().AppendQuery(nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(b)).To(Equal(urlValuesTest.query))
		}
	})
})

var _ = Describe("Pager", func() {
	It("Pager add limits and offsets to query", func() {
		query := `SELECT "select_test"."id", "select_test"."name" FROM "select_tests" AS "select_test"`

		urlValuesTests := []URLValuesTest{
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

		for _, urlValuesTest := range urlValuesTests {
			req, _ := http.NewRequest("GET", urlValuesTest.url, nil)

			q := NewQuery(nil, &SelectTest{})
			q = q.Apply(Pager(req.URL.Query()))

			b, err := q.selectQuery().AppendQuery(nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(b)).To(Equal(urlValuesTest.query))
		}
	})
})
