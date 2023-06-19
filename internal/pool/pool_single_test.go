package pool_test

import (
	"context"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/go-pg/pg/v10/internal/pool"
)

var _ = Describe("SingleConnPool", func() {
	var p *pool.SingleConnPool

	BeforeEach(func() {
		p = pool.NewSingleConnPool(nil, &pool.Conn{})
	})

	It("remove a conn due to context is cancelled", func() {
		ctx, cl := context.WithCancel(context.TODO())
		cn, err := p.Get(nil)
		Expect(err).To(BeNil())
		Expect(cn).ToNot(BeNil())

		cl()
		p.Remove(ctx, cn, nil)
		cn, err = p.Get(nil)
		Expect(cn).To(BeNil())
		Expect(err).ToNot(BeNil())
	})

})
