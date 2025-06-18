package orm

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Comment - escape comment symbols", func() {
	It("only open sequence", func() {
		var res []byte
		c := "/* comment"

		s := appendComment(res, c)
		Expect(s).To(Equal([]byte("/* /\\* comment */ ")))
	})
	It("only close sequence", func() {
		var res []byte
		c := "comment */"

		s := appendComment(res, c)
		Expect(s).To(Equal([]byte("/* comment *\\/ */ ")))
	})
	It("open and close sequences", func() {
		var res []byte
		c := "/* comment */"

		s := appendComment(res, c)
		Expect(s).To(Equal([]byte("/* /\\* comment *\\/ */ ")))
	})
})
