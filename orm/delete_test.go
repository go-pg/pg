package orm

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Delete", func() {
	It("supports RETURNING", func() {
		q := NewQuery(nil, nil).
			Table("delete_test").
			Where("1 = 1").
			Returning("id")

		b, err := deleteQuery{Query: q}.AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`DELETE FROM "delete_test" WHERE (1 = 1) RETURNING "id"`))
	})
})
