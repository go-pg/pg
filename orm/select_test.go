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
	It("specifies all columns", func() {
		q := NewQuery(nil, &SelectTest{})

		b, err := selectQuery{q}.AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`SELECT "select_test"."id", "select_test"."name" FROM "select_tests" AS "select_test"`))
	})

	It("works without db", func() {
		q := NewQuery(nil).Where("hello = ?", "world")

		b, err := selectQuery{q}.AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal("SELECT * WHERE (hello = 'world')"))
	})
})
