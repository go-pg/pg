package pool_test

import (
	"context"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/go-pg/pg/v10/internal/pool"
)

var _ = Describe("StickyConnPool", func() {
	var p *pool.StickyConnPool

	BeforeEach(func() {
		p = pool.NewStickyConnPool(nil)
	})

	It("closes the pool", func() {
		err := p.Close()
		Expect(err).NotTo(HaveOccurred())

		_, err = p.Get(context.Background())
		Expect(err).To(Equal(pool.ErrClosed))

		err = p.Close()
		Expect(err).To(Equal(pool.ErrClosed))
	})
})
