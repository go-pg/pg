package orm

import (
	"net/url"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type URLValuesModel struct {
	Id   int
	Name string
}

type urlValuesTest struct {
	urlQuery string
	query    string
}

var _ = Describe("URLValues", func() {
	query := `SELECT "url_values_model"."id", "url_values_model"."name" FROM "url_values_models" AS "url_values_model"`
	urlValuesTests := []urlValuesTest{{
		urlQuery: "id__gt=1",
		query:    query + ` WHERE ("id" > '1')`,
	}, {
		urlQuery: "name__gte=Michael",
		query:    query + ` WHERE ("name" >= 'Michael')`,
	}, {
		urlQuery: "id__lt=10",
		query:    query + ` WHERE ("id" < '10')`,
	}, {
		urlQuery: "name__lte=Peter",
		query:    query + ` WHERE ("name" <= 'Peter')`,
	}, {
		urlQuery: "name__exclude=Peter",
		query:    query + ` WHERE ("name" != 'Peter')`,
	}, {
		urlQuery: "name__exclude=Mike&name__exclude=Peter",
		query:    query + ` WHERE ("name" NOT IN ('Mike','Peter'))`,
	}, {
		urlQuery: "name=Mike",
		query:    query + ` WHERE ("name" = 'Mike')`,
	}, {
		urlQuery: "name__ieq=mik_",
		query:    query + ` WHERE ("name" ILIKE 'mik_')`,
	}, {
		urlQuery: "name__match=(m|p).*",
		query:    query + ` WHERE ("name" SIMILAR TO '(m|p).*')`,
	}, {
		urlQuery: "name__include=Peter&name__include=Mike",
		query:    query + ` WHERE ("name" IN ('Peter','Mike'))`,
	}, {
		urlQuery: "name=Mike&name=Peter",
		query:    query + ` WHERE ("name" IN ('Mike','Peter'))`,
	}, {
		urlQuery: "invalid_field=1",
		query:    query,
	}}

	It("adds conditions to the query", func() {
		for _, test := range urlValuesTests {
			values, err := url.ParseQuery(test.urlQuery)
			Expect(err).NotTo(HaveOccurred())

			q := NewQuery(nil, &URLValuesModel{})
			q = q.Apply(URLFilters(values))

			b, err := selectQuery{q: q}.AppendQuery(nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(b)).To(Equal(test.query))
		}
	})
})

var _ = Describe("Pager", func() {
	query := `SELECT "url_values_model"."id", "url_values_model"."name" FROM "url_values_models" AS "url_values_model"`
	urlValuesTests := []urlValuesTest{{
		urlQuery: "limit=10",
		query:    query + " LIMIT 10",
	}, {
		urlQuery: "page=5",
		query:    query + ` LIMIT 100 OFFSET 400`,
	}, {
		urlQuery: "page=5&limit=20",
		query:    query + ` LIMIT 20 OFFSET 80`,
	}}

	It("adds limit and offset to the query", func() {
		for _, test := range urlValuesTests {
			values, err := url.ParseQuery(test.urlQuery)
			Expect(err).NotTo(HaveOccurred())

			q := NewQuery(nil, &URLValuesModel{})
			q = q.Apply(Pagination(values))

			b, err := selectQuery{q: q}.AppendQuery(nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(b)).To(Equal(test.query))
		}
	})
})
