package orm_test

import (
	"math"
	"testing"

	"gopkg.in/pg.v4/orm"
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

type formatTest struct {
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

var formatTests = []formatTest{
	{q: "?", wanted: "?"},
	{q: "?", args: args{uint64(math.MaxUint64)}, wanted: "18446744073709551615"},
	{q: "?", args: args{orm.Q("query")}, wanted: "query"},
	{q: "?", args: args{orm.F("field")}, wanted: `"field"`},
	{q: "?null_empty", args: args{structv}, wanted: `NULL`},
	{q: "?", args: args{structv}, wanted: `'{"Foo":"bar","NullEmpty":""}'`},
	{q: `\? ?`, args: args{1}, wanted: "? 1"},
	{q: "? ?foo ?", args: args{"one", "two", structv}, wanted: "'one' 'bar' 'two'"},
	{q: "?foo ?Meth", args: args{structv}, wanted: "'bar' 'value'"},
	{q: "?foo ?Meth ?Meth2", args: args{embeddedStructv}, wanted: "'bar' 'value' 'value2'"},
	{q: "? ? ?", args: args{"foo", "bar"}, wanterr: "pg: expected at least 3 parameters, got 2"},
	{q: "?bar", args: args{structv}, wanterr: `pg: can't map "bar" on orm_test.structFormatter`},
	{q: "?MethWithArgs", args: args{structv}, wanterr: `pg: can't map "MethWithArgs" on orm_test.structFormatter`},
	{q: "?MethWithCompositeReturn", args: args{structv}, wanterr: `pg: can't map "MethWithCompositeReturn" on orm_test.structFormatter`},
}

func TestFormatQuery(t *testing.T) {
	for i, test := range formatTests {
		got, err := orm.FormatQuery(test.q, test.args...)
		if test.wanterr != "" {
			if err == nil {
				t.Fatalf("expected error (q=%q args=%v)", test.q, test.args)
			}
			if err.Error() != test.wanterr {
				t.Fatalf("got %q, wanted %q", err.Error(), test.wanterr)
			}
			continue
		}
		if err != nil {
			t.Fatalf("test #%d failed: %s", i, err)
			continue
		}
		if string(got) != test.wanted {
			t.Fatalf("got %q, wanted %q (q=%q args=%v)", got, test.wanted, test.q, test.args)
		}
	}
}
