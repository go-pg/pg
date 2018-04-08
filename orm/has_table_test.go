package orm

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type HasTableModel struct{}

type HasTableModelWithName struct {
	tableName struct{} `sql:"custom_name"`
}

var _ = Describe("HasTable", func() {
	It("has table", func() {
		q := NewQuery(nil, &HasTableModel{})

		b, err := hasTableQuery{q: q}.AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal("SELECT count(*) FROM pg_tables WHERE schemaname = 'public' AND tablename = 'has_table_models'"))
	})

	It("has table with custom name", func() {
		q := NewQuery(nil, &HasTableModelWithName{})

		b, err := hasTableQuery{q: q}.AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal("SELECT count(*) FROM pg_tables WHERE schemaname = 'public' AND tablename = 'custom_name'"))
	})
})