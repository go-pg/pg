package pg_test

import (
	"math"
	"testing"

	"gopkg.in/pg.v2"
)

type structFormatter struct {
	Foo string
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
	structv         = &structFormatter{"bar"}
	embeddedStructv = &embeddedStructFormatter{structv}
)

var formattingTests = []formattingTest{
	{q: "?", wanted: "?"},
	{q: "?", args: args{uint64(math.MaxUint64)}, wanted: "18446744073709551615"},
	{q: "? ?foo ?", args: args{"one", "two", structv}, wanted: "'one' 'bar' 'two'"},
	{q: "?foo ?Meth", args: args{structv}, wanted: "'bar' 'value'"},
	{q: "?foo ?Meth ?Meth2", args: args{embeddedStructv}, wanted: "'bar' 'value' 'value2'"},
	{q: "", args: args{"foo", "bar"}, wanterr: "pg: expected 0 parameters, got 2"},
	{q: "? ? ?", args: args{"foo", "bar"}, wanterr: "pg: expected at least 3 parameters, got 2"},
	{q: "?bar", args: args{structv}, wanterr: `pg: cannot map "bar"`},
	{q: "?MethWithArgs", args: args{structv}, wanterr: `pg: cannot map "MethWithArgs"`},
	{q: "?MethWithCompositeReturn", args: args{structv}, wanterr: `pg: cannot map "MethWithCompositeReturn"`},
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
