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
	wanted := 344
	if size != wanted {
		t.Fatalf("got %d, wanted %d", size, wanted)
	}
}

type FormatModel struct {
	Foo string
}

func TestQueryFormatQuery(t *testing.T) {
	q := orm.NewQuery(nil, &FormatModel{"bar"})

	params := &struct {
		Foo string
	}{
		"not_bar",
	}
	b := q.FormatQuery(nil, "?foo ?TableAlias", params)

	wanted := `'not_bar' "format_model"`
	if string(b) != wanted {
		t.Fatalf("got %q, wanted %q", string(b), wanted)
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
