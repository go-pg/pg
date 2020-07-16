package orm

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type DropTableModel struct{}

var _ = Describe("DropTable", func() {
	It("drops table", func() {
		q := NewQuery(nil, &DropTableModel{})

		s := dropTableQueryString(q, nil)
		Expect(s).To(Equal(`DROP TABLE "drop_table_models"`))
	})

	It("drops table if exists", func() {
		q := NewQuery(nil, &DropTableModel{})

		s := dropTableQueryString(q, &DropTableOptions{IfExists: true})
		Expect(s).To(Equal(`DROP TABLE IF EXISTS "drop_table_models"`))
	})
})

func dropTableQueryString(q *Query, opt *DropTableOptions) string {
	qq := NewDropTableQuery(q, opt)
	return queryString(qq)
}
