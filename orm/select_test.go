package orm

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type SelectTest struct {
	Id   int
	Name string
}

var _ = Describe("Select", func() {
	It("works without db", func() {
		q := NewQuery(nil).Where("hello = ?", "world")

		b, err := selectQuery{q}.AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal("SELECT * WHERE (hello = 'world')"))
	})

	It("sets all columns", func() {
		q := NewQuery(nil, &SelectTest{})

		b, err := selectQuery{q}.AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`SELECT "select_test"."id", "select_test"."name" FROM "select_tests" AS "select_test"`))
	})

	It("supports WrapWith", func() {
		q := NewQuery(nil, &SelectTest{}).
			Where("cond1").
			WrapWith("wrapper").
			Table("wrapper").
			Where("cond2")

		b, err := selectQuery{q}.AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`WITH "wrapper" AS (SELECT "select_test"."id", "select_test"."name" FROM "select_tests" AS "select_test" WHERE (cond1)) SELECT * FROM "wrapper" WHERE (cond2)`))
	})
})

type orderTest struct {
	order string
	query string
}

var _ = Describe("Select Order", func() {
	orderTests := []orderTest{
		{"id", `"id"`},
		{"id asc", `"id" asc`},
		{"id desc", `"id" desc`},
		{"id ASC", `"id" ASC`},
		{"id DESC", `"id" DESC`},
		{"id ASC NULLS FIRST", `"id ASC NULLS FIRST"`},
	}

	It("sets order", func() {
		for _, test := range orderTests {
			q := NewQuery(nil).Order(test.order)

			b, err := selectQuery{q}.AppendQuery(nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(string(b)).To(Equal(`SELECT * ORDER BY ` + test.query))
		}
	})
})
