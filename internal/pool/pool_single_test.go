package pool_test

import (
	"context"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/whenspeakteam/pg/v9/internal/pool"
)

func TestGinkgo(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "pool")
}

var _ = Describe("SingleConnPool", func() {
	var p *pool.SingleConnPool

	BeforeEach(func() {
		p = pool.NewSingleConnPool(nil)
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
