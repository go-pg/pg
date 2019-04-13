package pg_test

import (
	"net"
	"time"

	"github.com/go-pg/pg"

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
		_ = db.Close()
	})

	It("implements Stringer", func() {
		Expect(ln.String()).To(Equal("Listener(test_channel)"))

		_ = ln.Channel()
		Expect(ln.String()).To(Equal("Listener(test_channel, gopg:ping)"))
	})

	It("reuses connection", func() {
		for i := 0; i < 100; i++ {
			_, _, err := ln.ReceiveTimeout(time.Nanosecond)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(MatchRegexp(".+ i/o timeout"))
		}

		st := db.PoolStats()
		Expect(st.Hits).To(Equal(uint32(0)))
		Expect(st.Misses).To(Equal(uint32(0)))
		Expect(st.Timeouts).To(Equal(uint32(0)))
		Expect(st.TotalConns).To(Equal(uint32(1)))
		Expect(st.IdleConns).To(Equal(uint32(0)))
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

	It("is closed when DB is closed", func() {
		wait := make(chan struct{}, 2)

		go func() {
			defer GinkgoRecover()

			wait <- struct{}{}
			_, _, err := ln.Receive()

			Expect(err.Error()).To(SatisfyAny(
				Equal("EOF"),
				MatchRegexp(`use of closed (file or )?network connection$`),
			))
			wait <- struct{}{}
		}()

		select {
		case <-wait:
			// ok
		case <-time.After(time.Second):
			Fail("timeout")
		}

		select {
		case <-wait:
			Fail("Receive is not blocked")
		case <-time.After(time.Second):
			// ok
		}

		Expect(db.Close()).To(BeNil())

		select {
		case <-wait:
			// ok
		case <-time.After(3 * time.Second):
			Fail("Listener is not closed")
		}

		_, _, err := ln.Receive()
		Expect(err).To(MatchError("pg: listener is closed"))

		err = ln.Close()
		Expect(err).To(MatchError("pg: listener is closed"))
	})

	It("returns an error on timeout", func() {
		channel, payload, err := ln.ReceiveTimeout(time.Second)
		Expect(err.(net.Error).Timeout()).To(BeTrue())
		Expect(channel).To(Equal(""))
		Expect(payload).To(Equal(""))
	})

	It("reconnects on bad connection", func() {
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
			_, _, lnerr := ln.Receive()
			Expect(lnerr).NotTo(HaveOccurred())
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

	It("supports concurrent Listen and Receive", func() {
		const N = 100

		wg := performAsync(N, func(_ int) {
			_, err := db.Exec("NOTIFY test_channel")
			Expect(err).NotTo(HaveOccurred())
		})

		done := make(chan struct{})
		go func() {
			defer GinkgoRecover()

			for i := 0; i < N; i++ {
				_, _, err := ln.ReceiveTimeout(5 * time.Second)
				Expect(err).NotTo(HaveOccurred())
			}
			close(done)
		}()

		for i := 0; i < N; i++ {
			err := ln.Listen("test_channel")
			Expect(err).NotTo(HaveOccurred())
		}

		select {
		case <-done:
			wg.Wait()
		case <-time.After(30 * time.Second):
			Fail("timeout")
		}
	})
})
