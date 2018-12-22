package types

import (
	"testing"
)

var arrayTests = []struct {
	s   string
	els []string
}{
	{`{}`, []string{}},
	{`{""}`, []string{""}},
	{`{"\\"}`, []string{`\`}},
	{`{"''"}`, []string{`'`}},
	{`{{"''\"{}"}}`, []string{`{"''\"{}"}`}},
	{`{"''\"{}"}`, []string{`'"{}`}},

	{"{1,2}", []string{"1", "2"}},
	{"{1,NULL}", []string{"1", ""}},
	{`{"1","2"}`, []string{"1", "2"}},
	{`{"{1}","{2}"}`, []string{"{1}", "{2}"}},

	{"{{1,2},{3}}", []string{"{1,2}", "{3}"}},
}

func TestArrayParser(t *testing.T) {
	for testi, test := range arrayTests {
		p := newArrayParser(NewBytesReader([]byte(test.s)))

		var got []string
		for {
			b, err := p.NextElem()
			if err != nil {
				if err == endOfArray {
					break
				}
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

var array = `{foo,bar,"some relatively long string","foo\""}`

func BenchmarkArrayParserArray(b *testing.B) {
	bb := []byte(array)
	rd := NewBytesReader(bb)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		rd.Reset(bb)
		p := newArrayParser(rd)
		for {
			_, err := p.NextElem()
			if err != nil {
				if err == endOfArray {
					break
				}
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkArrayParserSubArray(b *testing.B) {
	var bb []byte
	bb = append(bb, '{')
	for i := 0; i < 100; i++ {
		if i > 0 {
			bb = append(bb, ',')
		}
		bb = append(bb, array...)
	}
	bb = append(bb, '}')

	rd := NewBytesReader(bb)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		rd.Reset(bb)
		p := newArrayParser(rd)
		for {
			_, err := p.NextElem()
			if err != nil {
				if err == endOfArray {
					break
				}
				b.Fatal(err)
			}
		}
	}
}
