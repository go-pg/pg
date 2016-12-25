package orm

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type UpdateTest struct{}

var _ = Describe("Update", func() {
	It("supports WITH", func() {
		q := NewQuery(nil, &UpdateTest{}).
			WrapWith("wrapper").
			Model(&UpdateTest{}).
			Table("wrapper").
			Where("update_test.id = wrapper.id")

		b, err := updateQuery{q}.AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`WITH "wrapper" AS (SELECT  FROM "update_tests" AS "update_test") UPDATE "update_tests" AS "update_test" SET  FROM "wrapper" WHERE (update_test.id = wrapper.id)`))
	})
})
