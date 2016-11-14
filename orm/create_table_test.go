package orm

import (
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type CreateTableTest struct {
	Id        int
	Name      string
	Count     int8
	CreatedOn time.Time `sql:",type:timestamp"`
	UpdatedOn time.Time
}

type TableWithoutPK struct {
	Name  string
	Count int8
}

var _ = Describe("CreateTable", func() {
	It("creates new table", func() {
		b, err := createTableQuery{model: CreateTableTest{}}.AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`CREATE TABLE "create_table_tests" (id bigint, name text, count smallint, created_on timestamp, updated_on timestamptz, PRIMARY KEY (id))`))
	})

	It("creates new table without primary key", func() {
		b, err := createTableQuery{model: TableWithoutPK{}}.AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`CREATE TABLE "table_without_pks" (name text, count smallint)`))
	})
})
