package orm

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type DropTableModel struct{}

var _ = Describe("CreateTable", func() {
	It("drops table", func() {
		q := NewQuery(nil, &DropTableModel{})

		b, err := (&dropTableQuery{q: q}).AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`DROP TABLE "drop_table_models"`))
	})

	It("drops table if exists", func() {
		q := NewQuery(nil, &DropTableModel{})

		b, err := (&dropTableQuery{
			q:   q,
			opt: &DropTableOptions{IfExists: true},
		}).AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`DROP TABLE IF EXISTS "drop_table_models"`))
	})
})
