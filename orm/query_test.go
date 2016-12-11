package orm_test

import (
	"testing"
	"unsafe"

	"gopkg.in/pg.v5/orm"
)

func TestQuerySize(t *testing.T) {
	size := int(unsafe.Sizeof(orm.Query{}))
	wanted := 328
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
