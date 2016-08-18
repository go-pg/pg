package orm

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Query", func() {
	It("works without db", func() {
		q := NewQuery(nil)
		q = q.Where("hello = ?", "world")

		b, err := selectQuery{q}.AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal("SELECT * WHERE (hello = 'world')"))
	})
})
