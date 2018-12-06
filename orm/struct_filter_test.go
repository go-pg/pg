package orm

import (
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

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

	Omit []byte `pg:"-"`
}

var _ = Describe("structFilter", func() {
	It("omits empty fields", func() {
		f := newStructFilter(&Filter{})

		s := f.Where()
		Expect(s).To(BeEmpty())
	})

	It("constructs WHERE clause with filled filter", func() {
		f := newStructFilter(&Filter{
			Field:    "one",
			FieldNEQ: "two",
			FieldLT:  1,
			FieldLTE: 2,
			FieldGT:  3,
			FieldGTE: 4,

			Multi:    []string{"one", "two"},
			MultiNEQ: []int{3, 4},

			Time: time.Unix(0, 0),
		})

		s := f.Where()
		Expect(s).To(Equal(`field = 'one' AND field != 'two' AND field < 1 AND field <= 2 AND field > 3 AND field >= 4 AND multi = ANY('{"one","two"}') AND multi != ALL('{3,4}') AND time = '1970-01-01 00:00:00+00:00:00'`))
	})
})
