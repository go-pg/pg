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
		Database: "postgres",
	}
}

var _ = Describe("Collection", func() {
	var db *pg.DB

	BeforeEach(func() {
		db = pg.Connect(pgOptions())
	})

	AfterEach(func() {
		Expect(db.Close()).NotTo(HaveOccurred())
	})

	It("supports slice of values", func() {
		coll := []struct {
			Id int
		}{}
		_, err := db.Query(&coll, `
			WITH data (id) AS (VALUES (1), (2), (3))
			SELECT id FROM data
		`)
		Expect(err).NotTo(HaveOccurred())
		Expect(coll).To(HaveLen(3))
		Expect(coll[0].Id).To(Equal(1))
		Expect(coll[1].Id).To(Equal(2))
		Expect(coll[2].Id).To(Equal(3))
	})

	It("supports slice of pointers", func() {
		coll := []*struct {
			Id int
		}{}
		_, err := db.Query(&coll, `
			WITH data (id) AS (VALUES (1), (2), (3))
			SELECT id FROM data
		`)
		Expect(err).NotTo(HaveOccurred())
		Expect(coll).To(HaveLen(3))
		Expect(coll[0].Id).To(Equal(1))
		Expect(coll[1].Id).To(Equal(2))
		Expect(coll[2].Id).To(Equal(3))
	})

	It("supports Collection", func() {
		var coll pg.Ints
		_, err := db.Query(&coll, `
			WITH data (id) AS (VALUES (1), (2), (3))
			SELECT id FROM data
		`)
		Expect(err).NotTo(HaveOccurred())
		Expect(coll).To(HaveLen(3))
		Expect(coll[0]).To(Equal(int64(1)))
		Expect(coll[1]).To(Equal(int64(2)))
		Expect(coll[2]).To(Equal(int64(3)))
	})
})

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

	Describe("with WithTimeout", func() {
		It("slow query passes", func() {
			_, err := db.WithTimeout(time.Minute).Exec(`SELECT pg_sleep(1)`)
			Expect(err).NotTo(HaveOccurred())
		})
	})
})

var _ = Describe("Listener.ReceiveTimeout", func() {
	var db *pg.DB
	var ln *pg.Listener

	BeforeEach(func() {
		opt := pgOptions()
		opt.PoolSize = 1
		db = pg.Connect(opt)

		var err error
		ln, err = db.Listen("test_channel")
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		err := db.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("reuses connection", func() {
		for i := 0; i < 100; i++ {
			_, _, err := ln.ReceiveTimeout(time.Millisecond)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(MatchRegexp(".+ i/o timeout"))
		}
	})
})
