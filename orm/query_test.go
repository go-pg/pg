package orm_test

import (
	"testing"
	"unsafe"

	"github.com/go-pg/pg/orm"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestQuerySize(t *testing.T) {
	size := int(unsafe.Sizeof(orm.Query{}))
	wanted := 360
	if size != wanted {
		t.Fatalf("got %d, wanted %d", size, wanted)
	}
}

func TestQueryFormatQuery(t *testing.T) {
	type FormatModel struct {
		Foo string
		Bar string
	}

	q := orm.NewQuery(nil, &FormatModel{"foo", "bar"})

	params := &struct {
		Foo string
	}{
		"not_foo",
	}
	b := q.FormatQuery(nil, "?foo ?TableName ?TableAlias ?Columns", params)

	wanted := `'not_foo' "format_models" "format_model" "format_model"."foo", "format_model"."bar"`
	if string(b) != wanted {
		t.Fatalf("got `%s`, wanted `%s`", string(b), wanted)
	}
}

var _ = Describe("NewQuery", func() {
	It("works with nil db", func() {
		q := orm.NewQuery(nil)

		b, err := q.AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal("SELECT *"))
	})

	It("works with nil model", func() {
		type Model struct {
			Id int
		}
		q := orm.NewQuery(nil, (*Model)(nil))

		b, err := q.AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`SELECT "model"."id" FROM "models" AS "model"`))
	})
})
