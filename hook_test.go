package pg_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"gopkg.in/pg.v4"
	"gopkg.in/pg.v4/orm"
)

type HookTest struct {
	Id int

	afterSelect bool

	beforeCreate bool
	afterCreate  bool
}

func (t *HookTest) AfterSelect(db orm.DB) error {
	t.afterSelect = true
	return nil
}

func (t *HookTest) BeforeCreate(db orm.DB) error {
	t.beforeCreate = true
	return nil
}

func (t *HookTest) AfterCreate(db orm.DB) error {
	t.afterCreate = true
	return nil
}

var _ = Describe("HookTest", func() {
	var db *pg.DB

	BeforeEach(func() {
		db = pg.Connect(pgOptions())
	})

	AfterEach(func() {
		Expect(db.Close()).NotTo(HaveOccurred())
	})

	It("calls AfterSelect for struct", func() {
		var hook HookTest
		_, err := db.QueryOne(&hook, "SELECT 1 AS id")
		Expect(err).NotTo(HaveOccurred())
		Expect(hook.afterSelect).To(BeTrue())
	})

	It("calls AfterSelect for slice", func() {
		var hooks []HookTest
		_, err := db.Query(&hooks, "SELECT 1 AS id")
		Expect(err).NotTo(HaveOccurred())
		Expect(hooks).To(HaveLen(1))
		Expect(hooks[0].afterSelect).To(BeTrue())
	})

	It("calls BeforeInsert and AfterInsert", func() {
		_, err := db.Exec("CREATE TEMP TABLE hook_tests (id int)")
		Expect(err).NotTo(HaveOccurred())

		hook := &HookTest{
			Id: 1,
		}
		err = db.Create(&hook)
		Expect(err).NotTo(HaveOccurred())
		Expect(hook.beforeCreate).To(BeTrue())
		Expect(hook.afterCreate).To(BeTrue())
	})
})
