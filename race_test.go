package pg_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"gopkg.in/pg.v4"
)

var _ = Describe("DB race", func() {
	var db *pg.DB
	var C, N int

	BeforeEach(func() {
		db = pg.Connect(pgOptions())
		err := createTestSchema(db)
		Expect(err).NotTo(HaveOccurred())

		C, N = 10, 1000
		if testing.Short() {
			C = 4
			N = 100
		}
	})

	AfterEach(func() {
		err := db.Close()
		Expect(err).NotTo(HaveOccurred())
	})

	It("SelectOrCreate with OnConflict is race free", func() {
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
					SelectOrCreate()
				Expect(err).NotTo(HaveOccurred())
				Expect(a.ID).NotTo(BeZero())

				if i%2 == 0 {
					err := db.Delete(a)
					if err != pg.ErrNoRows {
						Expect(err).NotTo(HaveOccurred())
					}
				}
			}
		})

		count, err := db.Model(Author{}).Count()
		Expect(err).NotTo(HaveOccurred())
		Expect(count).To(Equal(1))
	})

	It("SelectOrCreate without OnConflict is race free", func() {
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
					SelectOrCreate()
				Expect(err).NotTo(HaveOccurred())
				Expect(a.ID).NotTo(BeZero())

				if i%2 == 0 {
					err := db.Delete(a)
					if err != pg.ErrNoRows {
						Expect(err).NotTo(HaveOccurred())
					}
				}
			}
		})

		count, err := db.Model(Author{}).Count()
		Expect(err).NotTo(HaveOccurred())
		Expect(count).To(Equal(1))
	})
})
