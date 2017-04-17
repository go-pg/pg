package pg_test

import (
	"testing"
	"time"

	"github.com/go-pg/pg"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func concurrency() (int, int) {
	if testing.Short() {
		return 4, 100
	}
	return 10, 1000
}

var _ = Describe("DB timeout race", func() {
	var db *pg.DB
	var C, N int

	BeforeEach(func() {
		opt := pgOptions()
		opt.DialTimeout = 10 * time.Millisecond
		opt.ReadTimeout = 10 * time.Millisecond
		opt.WriteTimeout = 10 * time.Millisecond
		opt.PoolTimeout = 10 * time.Millisecond

		db = pg.Connect(opt)

		C, N = concurrency()
	})

	AfterEach(func() {
		err := db.Close()
		Expect(err).NotTo(HaveOccurred())

		// Give Postgres some time to recover.
		time.Sleep(time.Second)
	})

	It("is race free", func() {
		perform(C, func(id int) {
			for i := 0; i < N; i++ {
				_, err := db.Exec("SELECT pg_sleep(1)")
				Expect(err).To(HaveOccurred())
			}
		})
	})
})

var _ = Describe("DB race", func() {
	var db *pg.DB
	var C, N int

	BeforeEach(func() {
		db = pg.Connect(pgOptions())

		err := createTestSchema(db)
		Expect(err).NotTo(HaveOccurred())

		C, N = concurrency()
	})

	AfterEach(func() {
		err := db.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("invalid Scan is race free", func() {
		perform(C, func(id int) {
			for i := 0; i < N; i++ {
				var n int
				if i%2 == 0 {
					_, err := db.QueryOne(pg.Scan(&n), "SELECT 1, 2")
					Expect(err).To(HaveOccurred())
				} else {
					_, err := db.QueryOne(pg.Scan(&n), "SELECT 123")
					Expect(err).NotTo(HaveOccurred())
					Expect(n).To(Equal(123))
				}
			}
		})
	})

	It("SelectOrInsert with OnConflict is race free", func() {
		perform(C, func(id int) {
			a := &Author{
				Name: "R. Scott Bakker",
			}
			for i := 0; i < N; i++ {
				a.ID = 0
				_, err := db.Model(a).
					Column("id").
					Where("name = ?name").
					OnConflict("DO NOTHING").
					Returning("id").
					SelectOrInsert(&a.ID)
				Expect(err).NotTo(HaveOccurred())
				Expect(a.ID).NotTo(BeZero())

				if i%(N/C) == 0 {
					err := db.Delete(a)
					if err != pg.ErrNoRows {
						Expect(err).NotTo(HaveOccurred())
					}
				}
			}
		})

		count, err := db.Model(&Author{}).Count()
		Expect(err).NotTo(HaveOccurred())
		Expect(count).To(Equal(1))
	})

	It("SelectOrInsert without OnConflict is race free", func() {
		perform(C, func(id int) {
			a := &Author{
				Name: "R. Scott Bakker",
			}
			for i := 0; i < N; i++ {
				a.ID = 0
				_, err := db.Model(a).
					Column("id").
					Where("name = ?name").
					Returning("id").
					SelectOrInsert(&a.ID)
				Expect(err).NotTo(HaveOccurred())
				Expect(a.ID).NotTo(BeZero())

				if i%(N/C) == 0 {
					err := db.Delete(a)
					if err != pg.ErrNoRows {
						Expect(err).NotTo(HaveOccurred())
					}
				}
			}
		})

		count, err := db.Model(&Author{}).Count()
		Expect(err).NotTo(HaveOccurred())
		Expect(count).To(Equal(1))
	})
})
