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

	beforeSelect int
	afterSelect  int

	beforeInsert int
	afterInsert  int

	beforeUpdate int
	afterUpdate  int

	beforeDelete int
	afterDelete  int
}

var _ orm.BeforeSelectHook = (*HookTest)(nil)
var _ orm.AfterSelectHook = (*HookTest)(nil)
var _ orm.BeforeInsertHook = (*HookTest)(nil)
var _ orm.AfterInsertHook = (*HookTest)(nil)
var _ orm.BeforeUpdateHook = (*HookTest)(nil)
var _ orm.AfterUpdateHook = (*HookTest)(nil)
var _ orm.BeforeDeleteHook = (*HookTest)(nil)
var _ orm.AfterDeleteHook = (*HookTest)(nil)

func (t *HookTest) BeforeSelect(q *orm.Query) (*orm.Query, error) {
	t.beforeSelect++
	return q, nil
}

func (t *HookTest) AfterSelect(q *orm.Query) (*orm.Query, error) {
	t.afterSelect++
	return q, nil
}

func (t *HookTest) BeforeInsert(q *orm.Query) (*orm.Query, error) {
	t.beforeInsert++
	return q, nil
}

func (t *HookTest) AfterInsert(q *orm.Query) (*orm.Query, error) {
	t.afterInsert++
	return q, nil
}

func (t *HookTest) BeforeUpdate(q *orm.Query) (*orm.Query, error) {
	t.beforeUpdate++
	return q, nil
}

func (t *HookTest) AfterUpdate(q *orm.Query) (*orm.Query, error) {
	t.afterUpdate++
	return q, nil
}

func (t *HookTest) BeforeDelete(q *orm.Query) (*orm.Query, error) {
	t.beforeDelete++
	return q, nil
}

func (t *HookTest) AfterDelete(q *orm.Query) (*orm.Query, error) {
	t.afterDelete++
	return q, nil
}

type queryHookTest struct {
	beforeQueryMethod func(context.Context, *pg.QueryEvent) (context.Context, error)
	afterQueryMethod  func(context.Context, *pg.QueryEvent) (context.Context, error)
}

func (e queryHookTest) BeforeQuery(c context.Context, evt *pg.QueryEvent) (context.Context, error) {
	return e.beforeQueryMethod(c, evt)
}

func (e queryHookTest) AfterQuery(c context.Context, evt *pg.QueryEvent) (context.Context, error) {
	return e.afterQueryMethod(c, evt)
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

	It("calls AfterSelect for a struct model", func() {
		var hook HookTest
		err := db.Model(&hook).Select()
		Expect(err).NotTo(HaveOccurred())
		Expect(hook.afterSelect).To(Equal(1))
	})

	It("calls AfterSelect for a slice model", func() {
		var hooks []HookTest
		err := db.Model(&hooks).Select()
		Expect(err).NotTo(HaveOccurred())
		Expect(hooks).To(HaveLen(1))
		Expect(hooks[0].afterSelect).To(Equal(1))
	})

	It("calls BeforeInsert and AfterInsert", func() {
		hook := HookTest{
			Id:    1,
			Value: "value",
		}
		err := db.Insert(&hook)
		Expect(err).NotTo(HaveOccurred())
		Expect(hook.beforeInsert).To(Equal(1))
		Expect(hook.afterInsert).To(Equal(1))
	})

	It("calls BeforeUpdate and AfterUpdate", func() {
		hook := HookTest{
			Id: 1,
		}
		err := db.Update(&hook)
		Expect(err).NotTo(HaveOccurred())
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
		beforeQuery := func(c context.Context, evt *pg.QueryEvent) (context.Context, error) {
			Expect(evt.DB).To(Equal(db))
			Expect(evt.Query).To(Equal("SELECT ?"))
			Expect(evt.Params).To(Equal([]interface{}{1}))
			Expect(evt.Result).To(BeNil())
			Expect(evt.Error).To(BeNil())

			q, err := evt.UnformattedQuery()
			Expect(err).NotTo(HaveOccurred())
			Expect(q).To(Equal("SELECT ?"))

			q, err = evt.FormattedQuery()
			Expect(err).NotTo(HaveOccurred())
			Expect(q).To(Equal("SELECT 1"))

			evt.Stash = map[interface{}]interface{}{
				"data": 1,
			}

			return c, nil
		}

		afterQuery := func(c context.Context, evt *pg.QueryEvent) (context.Context, error) {
			Expect(evt.DB).To(Equal(db))
			Expect(evt.Query).To(Equal("SELECT ?"))
			Expect(evt.Params).To(Equal([]interface{}{1}))
			Expect(evt.Result).NotTo(BeNil())
			Expect(evt.Error).NotTo(HaveOccurred())

			q, err := evt.UnformattedQuery()
			Expect(err).NotTo(HaveOccurred())
			Expect(q).To(Equal("SELECT ?"))

			q, err = evt.FormattedQuery()
			Expect(err).NotTo(HaveOccurred())
			Expect(q).To(Equal("SELECT 1"))

			Expect(evt.Stash["data"]).To(Equal(1))

			count++

			return c, nil
		}

		BeforeEach(func() {
			hookImpl := struct{ queryHookTest }{}
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
		beforeQuery := func(c context.Context, evt *pg.QueryEvent) (context.Context, error) {
			Expect(evt.DB).To(Equal(db))
			Expect(evt.Query).NotTo(BeNil())
			Expect(evt.Params).To(HaveLen(1))
			Expect(evt.Result).To(BeNil())
			Expect(evt.Error).To(BeNil())

			q, err := evt.UnformattedQuery()
			Expect(err).NotTo(HaveOccurred())
			Expect(q).To(Equal("SELECT ?"))

			q, err = evt.FormattedQuery()
			Expect(err).NotTo(HaveOccurred())
			Expect(q).To(Equal("SELECT 1"))

			evt.Stash = map[interface{}]interface{}{
				"data": 1,
			}

			return c, nil
		}

		afterQuery := func(c context.Context, evt *pg.QueryEvent) (context.Context, error) {
			Expect(evt.DB).To(Equal(db))
			Expect(evt.Query).NotTo(BeNil())
			Expect(evt.Params).To(HaveLen(1))
			Expect(evt.Result).NotTo(BeNil())
			Expect(evt.Error).NotTo(HaveOccurred())

			q, err := evt.UnformattedQuery()
			Expect(err).NotTo(HaveOccurred())
			Expect(q).To(Equal("SELECT ?"))

			q, err = evt.FormattedQuery()
			Expect(err).NotTo(HaveOccurred())
			Expect(q).To(Equal("SELECT 1"))

			Expect(evt.Stash["data"]).To(Equal(1))

			count++

			return c, nil
		}

		BeforeEach(func() {
			hookImpl := struct{ queryHookTest }{}
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

var _ orm.BeforeSelectHook = (*BeforeSelectQueryModel)(nil)

func (BeforeSelectQueryModel) BeforeSelect(q *orm.Query) (*orm.Query, error) {
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
