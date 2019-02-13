package pg_test

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/go-pg/pg"
	"github.com/go-pg/pg/orm"
)

type HookTest struct {
	Id    int
	Value string

	afterQuery  int
	afterSelect int

	beforeInsert int
	afterInsert  int

	beforeUpdate int
	afterUpdate  int

	beforeDelete int
	afterDelete  int
}

func (t *HookTest) AfterQuery(c context.Context, db orm.DB) error {
	t.afterQuery++
	return nil
}

func (t *HookTest) AfterSelect(c context.Context, db orm.DB) error {
	t.afterSelect++
	return nil
}

func (t *HookTest) BeforeInsert(c context.Context, db orm.DB) error {
	t.beforeInsert++
	return nil
}

func (t *HookTest) AfterInsert(c context.Context, db orm.DB) error {
	t.afterInsert++
	return nil
}

func (t *HookTest) BeforeUpdate(c context.Context, db orm.DB) error {
	t.beforeUpdate++
	return nil
}

func (t *HookTest) AfterUpdate(c context.Context, db orm.DB) error {
	t.afterUpdate++
	return nil
}

func (t *HookTest) BeforeDelete(c context.Context, db orm.DB) error {
	t.beforeDelete++
	return nil
}

func (t *HookTest) AfterDelete(c context.Context, db orm.DB) error {
	t.afterDelete++
	return nil
}

type eventHookTest struct {
	beforeQueryMethod func(*pg.QueryEvent)
	afterQueryMethod  func(*pg.QueryEvent)
}

func (e eventHookTest) BeforeQuery(event *pg.QueryEvent) {
	e.beforeQueryMethod(event)
}

func (e eventHookTest) AfterQuery(event *pg.QueryEvent) {
	e.afterQueryMethod(event)
}

var _ = Describe("HookTest", func() {
	var db *pg.DB

	BeforeEach(func() {
		db = pg.Connect(pgOptions())

		qs := []string{
			"CREATE TEMP TABLE hook_tests (id int, value text)",
			"INSERT INTO hook_tests VALUES (1, '')",
		}
		for _, q := range qs {
			_, err := db.Exec(q)
			Expect(err).NotTo(HaveOccurred())
		}
	})

	AfterEach(func() {
		Expect(db.Close()).NotTo(HaveOccurred())
	})

	It("calls AfterQuery for a struct", func() {
		var hook HookTest
		_, err := db.QueryOne(&hook, "SELECT 1 AS id")
		Expect(err).NotTo(HaveOccurred())
		Expect(hook.afterQuery).To(Equal(1))
		Expect(hook.afterSelect).To(Equal(0))
	})

	It("calls AfterQuery and AfterSelect for a struct model", func() {
		var hook HookTest
		err := db.Model(&hook).Select()
		Expect(err).NotTo(HaveOccurred())
		Expect(hook.afterQuery).To(Equal(1))
		Expect(hook.afterSelect).To(Equal(1))
	})

	It("calls AfterQuery for a slice", func() {
		var hooks []HookTest
		_, err := db.Query(&hooks, "SELECT 1 AS id")
		Expect(err).NotTo(HaveOccurred())
		Expect(hooks).To(HaveLen(1))
		Expect(hooks[0].afterQuery).To(Equal(1))
		Expect(hooks[0].afterSelect).To(Equal(0))
	})

	It("calls AfterQuery and AfterSelect for a slice model", func() {
		var hooks []HookTest
		err := db.Model(&hooks).Select()
		Expect(err).NotTo(HaveOccurred())
		Expect(hooks).To(HaveLen(1))
		Expect(hooks[0].afterQuery).To(Equal(1))
		Expect(hooks[0].afterSelect).To(Equal(1))
	})

	It("calls BeforeInsert and AfterInsert", func() {
		hook := HookTest{
			Id:    1,
			Value: "value",
		}
		err := db.Insert(&hook)
		Expect(err).NotTo(HaveOccurred())
		Expect(hook.afterQuery).To(Equal(0))
		Expect(hook.beforeInsert).To(Equal(1))
		Expect(hook.afterInsert).To(Equal(1))
	})

	It("calls BeforeUpdate and AfterUpdate", func() {
		hook := HookTest{
			Id: 1,
		}
		err := db.Update(&hook)
		Expect(err).NotTo(HaveOccurred())
		Expect(hook.afterQuery).To(Equal(0))
		Expect(hook.beforeUpdate).To(Equal(1))
		Expect(hook.afterUpdate).To(Equal(1))
	})

	It("does not call BeforeUpdate and AfterUpdate for nil model", func() {
		_, err := db.Model((*HookTest)(nil)).
			Set("value = 'new'").
			Where("id = 123").
			Update()
		Expect(err).NotTo(HaveOccurred())
	})

	It("calls BeforeDelete and AfterDelete", func() {
		hook := HookTest{
			Id: 1,
		}
		err := db.Delete(&hook)
		Expect(err).NotTo(HaveOccurred())
		Expect(hook.afterQuery).To(Equal(0))
		Expect(hook.beforeDelete).To(Equal(1))
		Expect(hook.afterDelete).To(Equal(1))
	})

	It("does not call BeforeDelete and AfterDelete for nil model", func() {
		_, err := db.Model((*HookTest)(nil)).
			Where("id = 123").
			Delete()
		Expect(err).NotTo(HaveOccurred())
	})
})

var _ = Describe("OnQueryEvent", func() {
	var db *pg.DB
	var count int

	BeforeEach(func() {
		db = pg.Connect(pgOptions())
		count = 0
	})

	AfterEach(func() {
		Expect(db.Close()).NotTo(HaveOccurred())
	})

	Describe("Query/Exec", func() {
		afterQuery := func(event *pg.QueryEvent) {
			Expect(event.DB).To(Equal(db))
			Expect(event.Query).To(Equal("SELECT ?"))
			Expect(event.Params).To(Equal([]interface{}{1}))
			Expect(event.Result).NotTo(BeNil())
			Expect(event.Error).NotTo(HaveOccurred())

			q, err := event.UnformattedQuery()
			Expect(err).NotTo(HaveOccurred())
			Expect(q).To(Equal("SELECT ?"))

			q, err = event.FormattedQuery()
			Expect(err).NotTo(HaveOccurred())
			Expect(q).To(Equal("SELECT 1"))
			Expect(event.Data["data"]).To(Equal(1))

			count++
		}

		beforeQuery := func(event *pg.QueryEvent) {
			Expect(event.DB).To(Equal(db))
			Expect(event.Query).To(Equal("SELECT ?"))
			Expect(event.Params).To(Equal([]interface{}{1}))
			Expect(event.Result).To(BeNil())
			Expect(event.Error).To(BeNil())

			q, err := event.UnformattedQuery()
			Expect(err).NotTo(HaveOccurred())
			Expect(q).To(Equal("SELECT ?"))

			q, err = event.FormattedQuery()
			Expect(err).NotTo(HaveOccurred())
			Expect(q).To(Equal("SELECT 1"))
			event.Data["data"] = 1
		}

		BeforeEach(func() {
			hookImpl := struct{ eventHookTest }{}
			hookImpl.afterQueryMethod = afterQuery
			hookImpl.beforeQueryMethod = beforeQuery
			db.AddQueryHook(hookImpl)
		})

		It("is called for Query", func() {
			_, err := db.Query(pg.Discard, "SELECT ?", 1)
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(1))
		})

		It("is called for Exec", func() {
			_, err := db.Exec("SELECT ?", 1)
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(1))
		})
	})

	Describe("Model", func() {
		afterQuery := func(event *pg.QueryEvent) {
			Expect(event.DB).To(Equal(db))
			Expect(event.Query).NotTo(BeNil())
			Expect(event.Params).To(HaveLen(1))
			Expect(event.Result).NotTo(BeNil())
			Expect(event.Error).NotTo(HaveOccurred())

			q, err := event.UnformattedQuery()
			Expect(err).NotTo(HaveOccurred())
			Expect(q).To(Equal("SELECT ?"))

			q, err = event.FormattedQuery()
			Expect(err).NotTo(HaveOccurred())
			Expect(q).To(Equal("SELECT 1"))
			Expect(event.Data["data"]).To(Equal(1))

			count++
		}

		beforeQuery := func(event *pg.QueryEvent) {
			Expect(event.DB).To(Equal(db))
			Expect(event.Query).NotTo(BeNil())
			Expect(event.Params).To(HaveLen(1))
			Expect(event.Result).To(BeNil())
			Expect(event.Error).To(BeNil())

			q, err := event.UnformattedQuery()
			Expect(err).NotTo(HaveOccurred())
			Expect(q).To(Equal("SELECT ?"))

			q, err = event.FormattedQuery()
			Expect(err).NotTo(HaveOccurred())
			Expect(q).To(Equal("SELECT 1"))
			event.Data["data"] = 1
		}

		BeforeEach(func() {
			hookImpl := struct{ eventHookTest }{}
			hookImpl.afterQueryMethod = afterQuery
			hookImpl.beforeQueryMethod = beforeQuery
			db.AddQueryHook(hookImpl)
		})

		It("is called for Model", func() {
			var n int
			err := db.Model().ColumnExpr("?", 1).Select(&n)
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(1))
		})
	})
})

type BeforeSelectQueryModel struct {
	Id        int
	DeletedAt time.Time
}

func (BeforeSelectQueryModel) BeforeSelectQuery(
	c context.Context, db orm.DB, q *orm.Query,
) (*orm.Query, error) {
	q = q.Where("?TableAlias.deleted_at IS NULL")
	return q, nil
}

var _ = Describe("BeforeSelectQueryModel", func() {
	var db *pg.DB

	BeforeEach(func() {
		db = pg.Connect(pgOptions())

		err := db.CreateTable((*BeforeSelectQueryModel)(nil), &orm.CreateTableOptions{
			Temp: true,
		})
		Expect(err).NotTo(HaveOccurred())

		models := []BeforeSelectQueryModel{
			{Id: 1},
			{Id: 2, DeletedAt: time.Now()},
		}
		_, err = db.Model(&models).Insert()
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(db.Close()).NotTo(HaveOccurred())
	})

	It("applies BeforeSelectQuery hook", func() {
		var models []BeforeSelectQueryModel
		err := db.Model(&models).Select()
		Expect(err).NotTo(HaveOccurred())
		Expect(models).To(Equal([]BeforeSelectQueryModel{
			{Id: 1},
		}))
	})
})
