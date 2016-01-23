package types_test

import (
	"testing"

	"gopkg.in/pg.v3/types"
)

var appendFieldTests = []struct {
	field  string
	wanted string
}{
	{"", ""},
	{"id", `"id"`},
	{"table.id", `"table"."id"`},
	{"table.*", `"table".*`},

	{"id AS pk", `"id" AS "pk"`},
	{"table.id AS table__id", `"table"."id" AS "table__id"`},

	{"?PK", `?PK`},
	{`\?PK`, `"?PK"`},

	{`"`, `""""`},
	{`'`, `"'"`},
}

func TestAppendField(t *testing.T) {
	for _, test := range appendFieldTests {
		got := types.AppendField(nil, test.field, true)
		if string(got) != test.wanted {
			t.Errorf("got %q, wanted %q (field=%q)", got, test.wanted, test.field)
		}
	}
}
