package orm

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestQueryFormatQuery(t *testing.T) {
	type FormatModel struct {
		Foo string
		Bar string
	}

	q := NewQuery(nil, &FormatModel{"foo", "bar"})

	params := &struct {
		Foo string
	}{
		"not_foo",
	}
	fmter := NewFormatter().WithModel(q)
	b := fmter.FormatQuery(nil, "?foo ?TableName ?TableAlias ?TableColumns ?Columns", params)

	wanted := `'not_foo' "format_models" "format_model" "format_model"."foo", "format_model"."bar" "foo", "bar"`
	if string(b) != wanted {
		t.Fatalf("got `%s`, wanted `%s`", string(b), wanted)
	}
}

var _ = Describe("NewQuery", func() {
	It("works with nil db", func() {
		q := NewQuery(nil)

		b, err := q.AppendQuery(defaultFmter, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal("SELECT *"))
	})

	It("works with nil model", func() {
		type Model struct {
			Id int
		}
		q := NewQuery(nil, (*Model)(nil))

		b, err := q.AppendQuery(defaultFmter, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`SELECT "model"."id" FROM "models" AS "model"`))
	})
})
