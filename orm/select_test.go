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

	It("supports multiple groups", func() {
		q := NewQuery(nil).Group("one").Group("two")
		b, err := selectQuery{q}.AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`SELECT * GROUP BY "one", "two"`))
	})
})

var _ = Describe("With", func() {
	It("WrapWith wraps query in CTE", func() {
		q := NewQuery(nil, &SelectTest{}).
			Where("cond1").
			WrapWith("wrapper").
			Table("wrapper").
			Where("cond2")

		b, err := selectQuery{q}.AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`WITH "wrapper" AS (SELECT "select_test"."id", "select_test"."name" FROM "select_tests" AS "select_test" WHERE (cond1)) SELECT * FROM "wrapper" WHERE (cond2)`))
	})

	It("generates nested CTE", func() {
		q1 := NewQuery(nil).Table("q1")
		q2 := NewQuery(nil).With("q1", q1).Table("q2", "q1")
		q3 := NewQuery(nil).With("q2", q2).Table("q3", "q2")

		b, err := selectQuery{q3}.AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`WITH "q2" AS (WITH "q1" AS (SELECT * FROM "q1") SELECT * FROM "q2", "q1") SELECT * FROM "q3", "q2"`))
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
