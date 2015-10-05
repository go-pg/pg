package pg_test

import (
	"net"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"gopkg.in/pg.v3"
)

func TestPG(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "pg")
}

func pgOptions() *pg.Options {
	return &pg.Options{
		User:     "postgres",
		Database: "test",
	}
}

var _ = Describe("read/write timeout", func() {
	var db *pg.DB

	BeforeEach(func() {
		opt := pgOptions()
		opt.ReadTimeout = time.Millisecond
		db = pg.Connect(opt)
	})

	AfterEach(func() {
		Expect(db.Close()).NotTo(HaveOccurred())
	})

	It("slow query timeouts", func() {
		_, err := db.Exec(`SELECT pg_sleep(1)`)
		Expect(err.(net.Error).Timeout()).To(BeTrue())
	})

	Describe("with UseTimeout", func() {
		It("slow query passes", func() {
			_, err := db.UseTimeout(time.Minute).Exec(`SELECT pg_sleep(1)`)
			Expect(err).NotTo(HaveOccurred())
		})
	})
})

var _ = Describe("db.Conn", func() {
	var db *pg.DB

	BeforeEach(func() {
		opt := pgOptions()
		opt.PoolSize = 1
		db = pg.Connect(opt)
	})

	AfterEach(func() {
		err := db.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("reserves connection", func() {
		cn, err := db.Conn()
		Expect(err).NotTo(HaveOccurred())
		defer cn.Close()

		_, err = cn.Exec(`SET statement_timeout = 12345`)
		Expect(err).NotTo(HaveOccurred())

		var timeout pg.Strings
		_, err = cn.Query(&timeout, `SHOW statement_timeout`)
		Expect(err).NotTo(HaveOccurred())
		Expect(timeout[0]).To(Equal("12345ms"))
	})

	It("frees connection on close", func() {
		for i := 0; i < 10; i++ {
			cn, err := db.Conn()
			Expect(err).NotTo(HaveOccurred())

			_, err = cn.Exec(`SELECT 1`)
			Expect(err).NotTo(HaveOccurred())

			err = cn.Close()
			Expect(err).NotTo(HaveOccurred())
		}
	})

	It("works with transactions", func() {
		for i := 0; i < 10; i++ {
			cn, err := db.Conn()
			Expect(err).NotTo(HaveOccurred())

			_, err = cn.Exec(`BEGIN`)
			Expect(err).NotTo(HaveOccurred())

			err = cn.Close()
			Expect(err).NotTo(HaveOccurred())
		}
	})
})
