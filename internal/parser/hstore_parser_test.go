package parser_test

import (
	"testing"

	"gopkg.in/pg.v5/internal/parser"
)

var hstoreTests = []struct {
	s string
	m map[string]string
}{
	{`""=>""`, map[string]string{"": ""}},
	{`"k''k"=>"k''k"`, map[string]string{"k'k": "k'k"}},
	{`"k\"k"=>"k\"k"`, map[string]string{`k"k`: `k"k`}},
	{`"k\k"=>"k\k"`, map[string]string{`k\k`: `k\k`}},

	{`"foo"=>"bar"`, map[string]string{"foo": "bar"}},
	{`"foo"=>"bar","k"=>"v"`, map[string]string{"foo": "bar", "k": "v"}},
}

func TestHstoreParser(t *testing.T) {
	for testi, test := range hstoreTests {
		p := parser.NewHstoreParser([]byte(test.s))

		got := make(map[string]string)
		for p.Valid() {
			key, err := p.NextKey()
			if err != nil {
				t.Fatal(err)
			}

			value, err := p.NextValue()
			if err != nil {
				t.Fatal(err)
			}

			got[string(key)] = string(value)
		}

		if len(got) != len(test.m) {
			t.Fatalf(
				"#%d got %d elements, wanted %d (got=%#v wanted=%#v)",
				testi, len(got), len(test.m), got, test.m,
			)
		}

		for k, v := range got {
			if v != test.m[k] {
				t.Fatalf(
					"#%d el %q does not match: %q != %q (got=%#v wanted=%#v)",
					testi, k, v, test.m[k], got, test.m,
				)
			}
		}
	}
}
