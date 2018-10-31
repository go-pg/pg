package types

import (
	"testing"

	"github.com/go-pg/pg/internal"
)

var arrayTests = []struct {
	s   string
	els []string
}{
	// {`{"\\"}`, []string{`\`}},
	// {`{"''"}`, []string{`'`}},
	// {`{{"''\"{}"}}`, []string{`{"''\"{}"}`}},
	// {`{"''\"{}"}`, []string{`'"{}`}},

	// {"{1,2}", []string{"1", "2"}},
	{"{1,NULL}", []string{"1", ""}},
	// {`{"1","2"}`, []string{"1", "2"}},
	// {`{"{1}","{2}"}`, []string{"{1}", "{2}"}},

	// {"{{1,2},{3}}", []string{"{1,2}", "{3}"}},
}

func TestArrayParser(t *testing.T) {
	for testi, test := range arrayTests {
		p := newArrayParser(internal.NewBytesReader([]byte(test.s)))

		var got []string
		for p.Valid() {
			b, err := p.NextElem()
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, string(b))
		}

		if len(got) != len(test.els) {
			t.Fatalf(
				"test #%d got %d elements, wanted %d (got=%#v wanted=%#v)",
				testi, len(got), len(test.els), got, test.els)
		}

		for i, el := range got {
			if el != test.els[i] {
				t.Fatalf(
					"test #%d el #%d does not match: %s != %s (got=%#v wanted=%#v)",
					testi, i, el, test.els[i], got, test.els)
			}
		}
	}
}
