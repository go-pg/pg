package orm

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type HasColumnModel struct {
	FooColumn string
}

var _ = Describe("HasColumn", func() {
	It("has column", func() {
		q := NewQuery(nil, &HasColumnModel{})

		b, err := hasColumnQuery{q: q, clmName: "foo_column"}.AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal("SELECT count(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='has_column_models' AND column_name='foo_column'"))
	})
})
