package pg_test

import (
	"context"
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
		C, N = concurrency()
		N = 100
	})

	AfterEach(func() {
		pool := db.Pool()
		Expect(pool.Len()).To(Equal(0))
		Expect(pool.IdleLen()).To(Equal(0))

		err := db.Close()
		Expect(err).NotTo(HaveOccurred())

		// Give Postgres some time to recover.
		time.Sleep(time.Second)
	})

	test := func() {
		It("is race free", func() {
			perform(C, func(id int) {
				for i := 0; i < N; i++ {
					_, err := db.Exec("SELECT pg_sleep(1)")
					Expect(err).To(HaveOccurred())
				}
			})
		})
	}

	Describe("dial timeout", func() {
		BeforeEach(func() {
			opt := pgOptions()
			opt.DialTimeout = time.Nanosecond
			db = pg.Connect(opt)
		})

		test()
	})

	Describe("read timeout", func() {
		BeforeEach(func() {
			opt := pgOptions()
			opt.ReadTimeout = time.Nanosecond
			db = pg.Connect(opt)
		})

		test()
	})

	Describe("write timeout", func() {
		BeforeEach(func() {
			opt := pgOptions()
			opt.WriteTimeout = time.Nanosecond
			db = pg.Connect(opt)
		})

		test()
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

		count, err := db.Model((*Author)(nil)).Count()
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

		count, err := db.Model((*Author)(nil)).Count()
		Expect(err).NotTo(HaveOccurred())
		Expect(count).To(Equal(1))
	})

	It("WithContext is race free", func() {
		perform(C, func(id int) {
			dbWithCtx := db.WithContext(context.Background())
			Expect(dbWithCtx).NotTo(BeNil())
		})
	})

	It("WithTimeout is race free", func() {
		perform(C, func(id int) {
			dbWithTimeout := db.WithTimeout(5 * time.Second)
			Expect(dbWithTimeout).NotTo(BeNil())
		})
	})

	It("context timeout is race free", func() {
		perform(C, func(id int) {
			for i := 0; i < N; i++ {
				func() {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
					defer cancel()
					_, err := db.ExecContext(ctx, "SELECT 1")
					Expect(err).NotTo(HaveOccurred())
				}()
			}
		})
	})

	It("fully initializes model table", func() {
		type TestTable struct {
			tableName struct{} `sql:"'generate_series(0, 9)'"`
		}

		perform(C, func(id int) {
			n, err := db.Model((*TestTable)(nil)).Count()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(10))
		})
	})
})
