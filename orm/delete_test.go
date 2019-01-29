package orm

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type DeleteTest struct{}

var _ = Describe("Delete", func() {
	It("supports WITH", func() {
		q := NewQuery(nil, &DeleteTest{}).
			WrapWith("wrapper").
			Model(&DeleteTest{}).
			Table("wrapper").
			Where("delete_test.id = wrapper.id")

		b, err := (&deleteQuery{q: q}).AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`WITH "wrapper" AS (SELECT  FROM "delete_tests" AS "delete_test") DELETE FROM "delete_tests" AS "delete_test" USING "wrapper" WHERE (delete_test.id = wrapper.id)`))
	})
})
