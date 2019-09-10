package urlfilter_test

import (
	"database/sql"
	"encoding"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/go-pg/pg/v9/urlfilter"
)

type CustomField struct {
	s string
}

var _ encoding.TextUnmarshaler = (*CustomField)(nil)

func (f *CustomField) UnmarshalText(text []byte) error {
	f.s = string(text)
	return nil
}

type Filter struct {
	Field    string
	FieldNEQ string
	FieldLT  int8
	FieldLTE int16
	FieldGT  int32
	FieldGTE int64

	Multi    []string
	MultiNEQ []int

	Time time.Time

	NullBool    sql.NullBool
	NullInt64   sql.NullInt64
	NullFloat64 sql.NullFloat64
	NullString  sql.NullString

	Map    map[string]string
	Custom CustomField

	Omit []byte `pg:"-"`
}

var _ = Describe("Decode", func() {
	It("decodes struct from Values", func() {
		f := &Filter{}
		err := urlfilter.Decode(f, urlfilter.Values{
			"field":      {"one"},
			"field__neq": {"two"},
			"field__lt":  {"1"},
			"field__lte": {"2"},
			"field__gt":  {"3"},
			"field__gte": {"4"},

			"multi":      {"one", "two"},
			"multi__neq": {"3", "4"},

			"time": {"1970-01-01 00:00:00+00:00:00"},

			"null_bool":    {"t"},
			"null_int64":   {"1234"},
			"null_float64": {"1.234"},
			"null_string":  {"string"},

			"map[foo]":   {`bar`},
			"map[hello]": {`world`},
			"map[]":      {"invalid"},
			"map][":      {"invalid"},

			"custom": {"custom"},
		})
		Expect(err).NotTo(HaveOccurred())

		Expect(f.Field).To(Equal("one"))
		Expect(f.FieldNEQ).To(Equal("two"))
		Expect(f.FieldLT).To(Equal(int8(1)))
		Expect(f.FieldLTE).To(Equal(int16(2)))
		Expect(f.FieldGT).To(Equal(int32(3)))
		Expect(f.FieldGTE).To(Equal(int64(4)))

		Expect(f.Multi).To(Equal([]string{"one", "two"}))
		Expect(f.MultiNEQ).To(Equal([]int{3, 4}))

		Expect(f.Time).To(BeTemporally("==", time.Unix(0, 0)))

		Expect(f.NullBool.Valid).To(BeTrue())
		Expect(f.NullBool.Bool).To(BeTrue())

		Expect(f.NullInt64.Valid).To(BeTrue())
		Expect(f.NullInt64.Int64).To(Equal(int64(1234)))

		Expect(f.NullFloat64.Valid).To(BeTrue())
		Expect(f.NullFloat64.Float64).To(Equal(float64(1.234)))

		Expect(f.NullString.Valid).To(BeTrue())
		Expect(f.NullString.String).To(Equal("string"))

		Expect(f.Map).To(Equal(map[string]string{"foo": "bar", "hello": "world"}))
		Expect(f.Custom.s).To(Equal("custom"))
	})

	It("supports names with suffix `[]`", func() {
		f := &Filter{}
		err := urlfilter.Decode(f, urlfilter.Values{
			"field[]": {"one"},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(f.Field).To(Equal("one"))
	})

	It("supports names with prefix `:`", func() {
		f := &Filter{}
		err := urlfilter.Decode(f, urlfilter.Values{
			":field": {"one"},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(f.Field).To(Equal("one"))
	})

	It("decodes sql.Null*", func() {
		f := &Filter{}
		err := urlfilter.Decode(f, urlfilter.Values{
			"null_bool":    {""},
			"null_int64":   {""},
			"null_float64": {""},
			"null_string":  {""},
		})
		Expect(err).NotTo(HaveOccurred())

		Expect(f.NullBool.Valid).To(BeTrue())
		Expect(f.NullBool.Bool).To(BeZero())

		Expect(f.NullInt64.Valid).To(BeTrue())
		Expect(f.NullInt64.Int64).To(BeZero())

		Expect(f.NullFloat64.Valid).To(BeTrue())
		Expect(f.NullFloat64.Float64).To(BeZero())

		Expect(f.NullString.Valid).To(BeTrue())
		Expect(f.NullString.String).To(BeZero())
	})
})
