package pg_test

import (
	"net"
	"time"

	"gopkg.in/pg.v5"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Context("Listener", func() {
	var db *pg.DB
	var ln *pg.Listener

	BeforeEach(func() {
		opt := pgOptions()
		opt.PoolSize = 2
		opt.PoolTimeout = time.Second

		db = pg.Connect(opt)

		ln = db.Listen("test_channel")
	})

	var _ = AfterEach(func() {
		_ = ln.Close()

		err := db.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("reuses connection", func() {
		for i := 0; i < 100; i++ {
			_, _, err := ln.ReceiveTimeout(time.Nanosecond)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(MatchRegexp(".+ i/o timeout"))
		}

		st := db.Pool().Stats()
		Expect(st.Requests).To(Equal(uint32(1)))
		Expect(st.Hits).To(Equal(uint32(0)))
		Expect(st.Timeouts).To(Equal(uint32(0)))
		Expect(st.TotalConns).To(Equal(uint32(1)))
		Expect(st.FreeConns).To(Equal(uint32(0)))
	})

	It("listens for notifications", func() {
		wait := make(chan struct{}, 2)
		go func() {
			defer GinkgoRecover()

			wait <- struct{}{}
			channel, payload, err := ln.Receive()
			Expect(err).NotTo(HaveOccurred())
			Expect(channel).To(Equal("test_channel"))
			Expect(payload).To(Equal(""))
			wait <- struct{}{}
		}()

		select {
		case <-wait:
			// ok
		case <-time.After(3 * time.Second):
			Fail("timeout")
		}

		_, err := db.Exec("NOTIFY test_channel")
		Expect(err).NotTo(HaveOccurred())

		select {
		case <-wait:
			// ok
		case <-time.After(3 * time.Second):
			Fail("timeout")
		}
	})

	It("is aborted when DB is closed", func() {
		wait := make(chan struct{}, 2)

		go func() {
			defer GinkgoRecover()

			wait <- struct{}{}
			_, _, err := ln.Receive()

			Expect(err.Error()).Should(MatchRegexp(`^(.*use of closed network connection|EOF)$`))
			wait <- struct{}{}
		}()

		select {
		case <-wait:
			// ok
		case <-time.After(3 * time.Second):
			Fail("timeout")
		}

		select {
		case <-wait:
			Fail("Receive is not blocked")
		case <-time.After(time.Second):
			// ok
		}

		Expect(ln.Close()).To(BeNil())

		select {
		case <-wait:
			// ok
		case <-time.After(3 * time.Second):
			Fail("timeout")
		}
	})

	It("returns an error on timeout", func() {
		channel, payload, err := ln.ReceiveTimeout(time.Second)
		Expect(err.(net.Error).Timeout()).To(BeTrue())
		Expect(channel).To(Equal(""))
		Expect(payload).To(Equal(""))
	})

	It("reconnects on listen error", func() {
		cn := ln.CurrentConn()
		Expect(cn).NotTo(BeNil())
		cn.SetNetConn(&badConn{})

		err := ln.Listen("test_channel2")
		Expect(err).Should(MatchError("bad connection"))

		err = ln.Listen("test_channel2")
		Expect(err).NotTo(HaveOccurred())
	})

	It("reconnects on receive error", func() {
		cn := ln.CurrentConn()
		Expect(cn).NotTo(BeNil())
		cn.SetNetConn(&badConn{})

		_, _, err := ln.ReceiveTimeout(time.Second)
		Expect(err).Should(MatchError("bad connection"))

		_, _, err = ln.ReceiveTimeout(time.Second)
		Expect(err.(net.Error).Timeout()).To(BeTrue())

		wait := make(chan struct{}, 2)
		go func() {
			defer GinkgoRecover()

			wait <- struct{}{}
			_, _, err := ln.Receive()
			Expect(err).NotTo(HaveOccurred())
			wait <- struct{}{}
		}()

		select {
		case <-wait:
			// ok
		case <-time.After(3 * time.Second):
			Fail("timeout")
		}

		select {
		case <-wait:
			Fail("Receive is not blocked")
		case <-time.After(time.Second):
			// ok
		}

		_, err = db.Exec("NOTIFY test_channel")
		Expect(err).NotTo(HaveOccurred())

		select {
		case <-wait:
			// ok
		case <-time.After(3 * time.Second):
			Fail("timeout")
		}
	})
})
