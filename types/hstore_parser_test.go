package types

import (
	"testing"

	"github.com/go-pg/pg/internal"
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
		got, err := scanMapStringString(internal.NewBytesReader([]byte(test.s)), 0)
		if err != nil {
			t.Fatal(err)
		}

		if len(got) != len(test.m) {
			t.Fatalf(
				"test #%d got %d elements, wanted %d (got=%#v wanted=%#v)",
				testi, len(got), len(test.m), got, test.m)
		}

		for k, v := range got {
			if v != test.m[k] {
				t.Fatalf(
					"#%d el %q does not match: %s != %s (got=%#v wanted=%#v)",
					testi, k, v, test.m[k], got, test.m)
			}
		}
	}
}
