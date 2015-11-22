package pg_test

import (
	"math"
	"testing"

	"gopkg.in/pg.v3"
)

type structFormatter struct {
	Foo       string
	NullEmpty string `pg:",nullempty"`
}

func (structFormatter) Meth() string {
	return "value"
}

func (structFormatter) MethWithArgs(string) string {
	return "value"
}

func (structFormatter) MethWithCompositeReturn() (string, string) {
	return "value1", "value2"
}

type embeddedStructFormatter struct {
	*structFormatter
}

func (embeddedStructFormatter) Meth2() string {
	return "value2"
}

type formattingTest struct {
	q       string
	args    []interface{}
	wanted  string
	wanterr string
}

type args []interface{}

var (
	structv         = &structFormatter{Foo: "bar"}
	embeddedStructv = &embeddedStructFormatter{structv}
)

var formattingTests = []formattingTest{
	{q: "?", wanted: "?"},
	{q: "?", args: args{uint64(math.MaxUint64)}, wanted: "18446744073709551615"},
	{q: "?", args: args{pg.Q("query")}, wanted: "query"},
	{q: "?", args: args{pg.F("field")}, wanted: `"field"`},
	{q: "?null_empty", args: args{structv}, wanted: `NULL`},
	{q: "?", args: args{structv}, wanted: `'{"Foo":"bar","NullEmpty":""}'`},
	{q: `\? ?`, args: args{1}, wanted: "? 1"},
	{q: "? ?foo ?", args: args{"one", "two", structv}, wanted: "'one' 'bar' 'two'"},
	{q: "?foo ?Meth", args: args{structv}, wanted: "'bar' 'value'"},
	{q: "?foo ?Meth ?Meth2", args: args{embeddedStructv}, wanted: "'bar' 'value' 'value2'"},
	{q: "", args: args{"foo", "bar"}, wanterr: "pg: expected 0 parameters, got 2"},
	{q: "? ? ?", args: args{"foo", "bar"}, wanterr: "pg: expected at least 3 parameters, got 2"},
	{q: "?bar", args: args{structv}, wanterr: `pg: cannot map "bar" on *pg_test.structFormatter`},
	{q: "?MethWithArgs", args: args{structv}, wanterr: `pg: cannot map "MethWithArgs" on *pg_test.structFormatter`},
	{q: "?MethWithCompositeReturn", args: args{structv}, wanterr: `pg: cannot map "MethWithCompositeReturn" on *pg_test.structFormatter`},
}

func TestFormatting(t *testing.T) {
	for _, test := range formattingTests {
		got, err := pg.FormatQ(test.q, test.args...)
		if test.wanterr != "" {
			if err == nil {
				t.Errorf("expected error (q=%q args=%v)", test.q, test.args)
			}
			if err.Error() != test.wanterr {
				t.Errorf("got %q, wanted %q", err.Error(), test.wanterr)
			}
			continue
		}
		if err != nil {
			t.Error(err)
			continue
		}
		if string(got) != test.wanted {
			t.Errorf("got %q, wanted %q (q=%q args=%v)", got, test.wanted, test.q, test.args)
		}
	}
}
