package pg_test

import (
	"context"
	"io/ioutil"
	"strings"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/go-pg/pg/v10"
)

type HookTest struct {
	Id    int
	Value string

	beforeScan int
	afterScan  int

	afterSelect int

	beforeInsert int
	afterInsert  int

	beforeUpdate int
	afterUpdate  int

	beforeDelete int
	afterDelete  int
}

var _ pg.BeforeScanHook = (*HookTest)(nil)

func (t *HookTest) BeforeScan(c context.Context) error {
	t.beforeScan++
	return nil
}

var _ pg.AfterScanHook = (*HookTest)(nil)

func (t *HookTest) AfterScan(c context.Context) error {
	t.afterScan++
	return nil
}

var _ pg.AfterSelectHook = (*HookTest)(nil)

func (t *HookTest) AfterSelect(c context.Context) error {
	t.afterSelect++
	return nil
}

var _ pg.BeforeInsertHook = (*HookTest)(nil)

func (t *HookTest) BeforeInsert(c context.Context) (context.Context, error) {
	t.beforeInsert++
	return c, nil
}

var _ pg.AfterInsertHook = (*HookTest)(nil)

func (t *HookTest) AfterInsert(c context.Context) error {
	t.afterInsert++
	return nil
}

var _ pg.BeforeUpdateHook = (*HookTest)(nil)

func (t *HookTest) BeforeUpdate(c context.Context) (context.Context, error) {
	t.beforeUpdate++
	return c, nil
}

var _ pg.AfterUpdateHook = (*HookTest)(nil)

func (t *HookTest) AfterUpdate(c context.Context) error {
	t.afterUpdate++
	return nil
}

var _ pg.BeforeDeleteHook = (*HookTest)(nil)

func (t *HookTest) BeforeDelete(c context.Context) (context.Context, error) {
	t.beforeDelete++
	return c, nil
}

var _ pg.AfterDeleteHook = (*HookTest)(nil)

func (t *HookTest) AfterDelete(c context.Context) error {
	t.afterDelete++
	return nil
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
		Expect(hook).To(Equal(HookTest{
			Id:          1,
			beforeScan:  1,
			afterScan:   1,
			afterSelect: 1,
		}))
	})

	It("calls AfterSelect for a slice model", func() {
		var hooks []HookTest
		err := db.Model(&hooks).Select()
		Expect(err).NotTo(HaveOccurred())
		Expect(hooks).To(HaveLen(1))
		Expect(hooks[0]).To(Equal(HookTest{
			Id:          1,
			beforeScan:  1,
			afterScan:   1,
			afterSelect: 1,
		}))
	})

	It("calls BeforeInsert and AfterInsert", func() {
		hook := &HookTest{
			Id: 1,
		}
		_, err := db.Model(hook).Insert()
		Expect(err).NotTo(HaveOccurred())
		Expect(hook).To(Equal(&HookTest{
			Id:           1,
			beforeScan:   1,
			afterScan:    1,
			beforeInsert: 1,
			afterInsert:  1,
		}))
	})

	It("calls BeforeUpdate and AfterUpdate", func() {
		hook := &HookTest{
			Id: 1,
		}
		_, err := db.Model(hook).WherePK().Update()
		Expect(err).NotTo(HaveOccurred())
		Expect(hook).To(Equal(&HookTest{
			Id:           1,
			beforeUpdate: 1,
			afterUpdate:  1,
		}))
	})

	It("does not call BeforeUpdate and AfterUpdate for nil model", func() {
		_, err := db.Model((*HookTest)(nil)).
			Set("value = 'new'").
			Where("id = 123").
			Update()
		Expect(err).NotTo(HaveOccurred())
	})

	It("calls BeforeDelete and AfterDelete", func() {
		hook := &HookTest{
			Id: 1,
		}
		_, err := db.Model(hook).WherePK().Delete()
		Expect(err).NotTo(HaveOccurred())
		Expect(hook).To(Equal(&HookTest{
			Id:           1,
			beforeDelete: 1,
			afterDelete:  1,
		}))
	})

	It("does not call BeforeDelete and AfterDelete for nil model", func() {
		_, err := db.Model((*HookTest)(nil)).
			Where("id = 123").
			Delete()
		Expect(err).NotTo(HaveOccurred())
	})
})

type queryHookTest struct {
	beforeQueryMethod func(context.Context, *pg.QueryEvent) (context.Context, error)
	afterQueryMethod  func(context.Context, *pg.QueryEvent) error
}

func (e queryHookTest) BeforeQuery(c context.Context, evt *pg.QueryEvent) (context.Context, error) {
	return e.beforeQueryMethod(c, evt)
}

func (e queryHookTest) AfterQuery(c context.Context, evt *pg.QueryEvent) error {
	return e.afterQueryMethod(c, evt)
}

var _ = Describe("BeforeQuery and AfterQuery", func() {
	var db *pg.DB
	var count int

	BeforeEach(func() {
		db = pg.Connect(pgOptions())
		count = 0

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

	Describe("Query/Exec", func() {
		beforeQuery := func(c context.Context, evt *pg.QueryEvent) (context.Context, error) {
			Expect(evt.DB).To(Equal(db))
			Expect(evt.Query).To(Equal("SELECT ?"))
			Expect(evt.Params).To(Equal([]interface{}{1}))
			Expect(evt.Result).To(BeNil())
			Expect(evt.Err).To(BeNil())

			q, err := evt.UnformattedQuery()
			Expect(err).NotTo(HaveOccurred())
			Expect(string(q)).To(Equal("SELECT ?"))

			q, err = evt.FormattedQuery()
			Expect(err).NotTo(HaveOccurred())
			Expect(string(q)).To(Equal("SELECT 1"))

			evt.Stash = map[interface{}]interface{}{
				"data": 1,
			}

			return c, nil
		}

		afterQuery := func(c context.Context, evt *pg.QueryEvent) error {
			Expect(evt.DB).To(Equal(db))
			Expect(evt.Query).To(Equal("SELECT ?"))
			Expect(evt.Params).To(Equal([]interface{}{1}))
			Expect(evt.Result).NotTo(BeNil())
			Expect(evt.Err).NotTo(HaveOccurred())

			q, err := evt.UnformattedQuery()
			Expect(err).NotTo(HaveOccurred())
			Expect(string(q)).To(Equal("SELECT ?"))

			q, err = evt.FormattedQuery()
			Expect(err).NotTo(HaveOccurred())
			Expect(string(q)).To(Equal("SELECT 1"))

			Expect(evt.Stash["data"]).To(Equal(1))

			count++

			return nil
		}

		BeforeEach(func() {
			hookImpl := struct{ queryHookTest }{}
			hookImpl.beforeQueryMethod = beforeQuery
			hookImpl.afterQueryMethod = afterQuery
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
			Expect(evt.Err).To(BeNil())

			q, err := evt.UnformattedQuery()
			Expect(err).NotTo(HaveOccurred())
			Expect(string(q)).To(Equal("SELECT ?"))

			q, err = evt.FormattedQuery()
			Expect(err).NotTo(HaveOccurred())
			Expect(string(q)).To(Equal("SELECT 1"))

			evt.Stash = map[interface{}]interface{}{
				"data": 1,
			}

			return c, nil
		}

		afterQuery := func(c context.Context, evt *pg.QueryEvent) error {
			Expect(evt.DB).To(Equal(db))
			Expect(evt.Query).NotTo(BeNil())
			Expect(evt.Params).To(HaveLen(1))
			Expect(evt.Result).NotTo(BeNil())
			Expect(evt.Err).NotTo(HaveOccurred())

			q, err := evt.UnformattedQuery()
			Expect(err).NotTo(HaveOccurred())
			Expect(string(q)).To(Equal("SELECT ?"))

			q, err = evt.FormattedQuery()
			Expect(err).NotTo(HaveOccurred())
			Expect(string(q)).To(Equal("SELECT 1"))

			Expect(evt.Stash["data"]).To(Equal(1))

			count++

			return nil
		}

		BeforeEach(func() {
			hookImpl := struct{ queryHookTest }{}
			hookImpl.beforeQueryMethod = beforeQuery
			hookImpl.afterQueryMethod = afterQuery
			db.AddQueryHook(hookImpl)
		})

		It("is called for Model", func() {
			var n int
			err := db.Model().ColumnExpr("?", 1).Select(&n)
			Expect(err).NotTo(HaveOccurred())
			Expect(count).To(Equal(1))
		})
	})

	Describe("model params", func() {
		BeforeEach(func() {
			hookImpl := struct{ queryHookTest }{}
			hookImpl.beforeQueryMethod = func(c context.Context, evt *pg.QueryEvent) (context.Context, error) {
				return c, nil
			}
			hookImpl.afterQueryMethod = func(c context.Context, evt *pg.QueryEvent) error {
				q, err := evt.FormattedQuery()
				Expect(err).NotTo(HaveOccurred())
				Expect(string(q)).To(Equal(`CREATE INDEX stories_author_id_idx ON "hook_tests" (author_id)`))
				return nil
			}
			db.AddQueryHook(hookImpl)
		})

		It("is called for Model", func() {
			_, err := db.Model((*HookTest)(nil)).Exec("CREATE INDEX stories_author_id_idx ON ?TableName (author_id)")
			Expect(err).To(HaveOccurred())
		})
	})

	Describe("CopyTo", func() {
		It("is called for CopyTo with model", func() {
			hookImpl := struct{ queryHookTest }{}
			hookImpl.beforeQueryMethod = func(c context.Context, evt *pg.QueryEvent) (context.Context, error) {
				Expect(evt.DB).To(Equal(db))
				Expect(evt.Query).To(Equal(`COPY ?TableName TO STDOUT CSV`))
				Expect(evt.Model).NotTo(BeNil())
				Expect(evt.Params).To(ContainElement(evt.Model))
				Expect(evt.Err).To(BeNil())

				q, err := evt.UnformattedQuery()
				Expect(err).NotTo(HaveOccurred())
				Expect(string(q)).To(Equal(`COPY ?TableName TO STDOUT CSV`))

				q, err = evt.FormattedQuery()
				Expect(err).NotTo(HaveOccurred())
				Expect(string(q)).To(Equal(`COPY "hook_tests" TO STDOUT CSV`))

				evt.Stash = map[interface{}]interface{}{
					"data": 1,
				}

				return c, nil
			}

			hookImpl.afterQueryMethod = func(c context.Context, evt *pg.QueryEvent) error {
				Expect(evt.DB).To(Equal(db))
				Expect(evt.Query).To(Equal(`COPY ?TableName TO STDOUT CSV`))
				Expect(evt.Model).NotTo(BeNil())
				Expect(evt.Params).To(ContainElement(evt.Model))
				Expect(evt.Err).NotTo(HaveOccurred())

				q, err := evt.UnformattedQuery()
				Expect(err).NotTo(HaveOccurred())
				Expect(string(q)).To(Equal(`COPY ?TableName TO STDOUT CSV`))

				q, err = evt.FormattedQuery()
				Expect(err).NotTo(HaveOccurred())
				Expect(string(q)).To(Equal(`COPY "hook_tests" TO STDOUT CSV`))

				Expect(evt.Stash["data"]).To(Equal(1))

				return nil
			}
			db.AddQueryHook(hookImpl)

			_, err := db.Model((*HookTest)(nil)).CopyTo(ioutil.Discard, `COPY ?TableName TO STDOUT CSV`)
			Expect(err).NotTo(HaveOccurred())
		})

		It("is called for CopyTo without model", func() {
			hookImpl := struct{ queryHookTest }{}
			hookImpl.beforeQueryMethod = func(c context.Context, evt *pg.QueryEvent) (context.Context, error) {
				Expect(evt.DB).To(Equal(db))
				Expect(evt.Query).To(Equal(`COPY (SELECT 1) TO STDOUT CSV`))
				Expect(evt.Params).To(BeNil())
				Expect(evt.Err).To(BeNil())

				q, err := evt.UnformattedQuery()
				Expect(err).NotTo(HaveOccurred())
				Expect(string(q)).To(Equal(`COPY (SELECT 1) TO STDOUT CSV`))

				q, err = evt.FormattedQuery()
				Expect(err).NotTo(HaveOccurred())
				Expect(string(q)).To(Equal(`COPY (SELECT 1) TO STDOUT CSV`))

				evt.Stash = map[interface{}]interface{}{
					"data": 1,
				}

				return c, nil
			}

			hookImpl.afterQueryMethod = func(c context.Context, evt *pg.QueryEvent) error {
				Expect(evt.DB).To(Equal(db))
				Expect(evt.Query).To(Equal(`COPY (SELECT 1) TO STDOUT CSV`))
				Expect(evt.Params).To(BeNil())
				Expect(evt.Err).To(BeNil())

				q, err := evt.UnformattedQuery()
				Expect(err).NotTo(HaveOccurred())
				Expect(string(q)).To(Equal(`COPY (SELECT 1) TO STDOUT CSV`))

				q, err = evt.FormattedQuery()
				Expect(err).NotTo(HaveOccurred())
				Expect(string(q)).To(Equal(`COPY (SELECT 1) TO STDOUT CSV`))

				Expect(evt.Stash["data"]).To(Equal(1))

				return nil
			}
			db.AddQueryHook(hookImpl)

			_, err := db.CopyTo(ioutil.Discard, `COPY (SELECT 1) TO STDOUT CSV`)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("CopyFrom", func() {
		It("is called for CopyFrom with model", func() {
			hookImpl := struct{ queryHookTest }{}
			hookImpl.beforeQueryMethod = func(c context.Context, evt *pg.QueryEvent) (context.Context, error) {
				Expect(evt.DB).To(Equal(db))
				Expect(evt.Query).To(Equal(`COPY ?TableName FROM STDIN CSV`))
				Expect(evt.Model).NotTo(BeNil())
				Expect(evt.Params).To(ContainElement(evt.Model))
				Expect(evt.Err).To(BeNil())

				q, err := evt.UnformattedQuery()
				Expect(err).NotTo(HaveOccurred())
				Expect(string(q)).To(Equal(`COPY ?TableName FROM STDIN CSV`))

				q, err = evt.FormattedQuery()
				Expect(err).NotTo(HaveOccurred())
				Expect(string(q)).To(Equal(`COPY "hook_tests" FROM STDIN CSV`))

				evt.Stash = map[interface{}]interface{}{
					"data": 1,
				}

				return c, nil
			}

			hookImpl.afterQueryMethod = func(c context.Context, evt *pg.QueryEvent) error {
				Expect(evt.DB).To(Equal(db))
				Expect(evt.Query).To(Equal(`COPY ?TableName FROM STDIN CSV`))
				Expect(evt.Model).NotTo(BeNil())
				Expect(evt.Params).To(ContainElement(evt.Model))
				Expect(evt.Err).NotTo(HaveOccurred())

				q, err := evt.UnformattedQuery()
				Expect(err).NotTo(HaveOccurred())
				Expect(string(q)).To(Equal(`COPY ?TableName FROM STDIN CSV`))

				q, err = evt.FormattedQuery()
				Expect(err).NotTo(HaveOccurred())
				Expect(string(q)).To(Equal(`COPY "hook_tests" FROM STDIN CSV`))

				Expect(evt.Stash["data"]).To(Equal(1))

				return nil
			}
			db.AddQueryHook(hookImpl)

			const in = `10,test`
			_, err := db.Model((*HookTest)(nil)).CopyFrom(strings.NewReader(in), `COPY ?TableName FROM STDIN CSV`)
			Expect(err).NotTo(HaveOccurred())
		})

		It("is called for CopyFrom without model", func() {
			hookImpl := struct{ queryHookTest }{}
			hookImpl.beforeQueryMethod = func(c context.Context, evt *pg.QueryEvent) (context.Context, error) {
				Expect(evt.DB).To(Equal(db))
				Expect(evt.Query).To(Equal(`COPY "hook_tests" FROM STDIN CSV`))
				Expect(evt.Model).To(BeNil())
				Expect(evt.Err).To(BeNil())

				q, err := evt.UnformattedQuery()
				Expect(err).NotTo(HaveOccurred())
				Expect(string(q)).To(Equal(`COPY "hook_tests" FROM STDIN CSV`))

				q, err = evt.FormattedQuery()
				Expect(err).NotTo(HaveOccurred())
				Expect(string(q)).To(Equal(`COPY "hook_tests" FROM STDIN CSV`))

				evt.Stash = map[interface{}]interface{}{
					"data": 1,
				}

				return c, nil
			}

			hookImpl.afterQueryMethod = func(c context.Context, evt *pg.QueryEvent) error {
				Expect(evt.DB).To(Equal(db))
				Expect(evt.Query).To(Equal(`COPY "hook_tests" FROM STDIN CSV`))
				Expect(evt.Model).To(BeNil())
				Expect(evt.Err).NotTo(HaveOccurred())

				q, err := evt.UnformattedQuery()
				Expect(err).NotTo(HaveOccurred())
				Expect(string(q)).To(Equal(`COPY "hook_tests" FROM STDIN CSV`))

				q, err = evt.FormattedQuery()
				Expect(err).NotTo(HaveOccurred())
				Expect(string(q)).To(Equal(`COPY "hook_tests" FROM STDIN CSV`))

				Expect(evt.Stash["data"]).To(Equal(1))

				return nil
			}
			db.AddQueryHook(hookImpl)

			const in = `10,test`
			_, err := db.CopyFrom(strings.NewReader(in), `COPY "hook_tests" FROM STDIN CSV`)
			Expect(err).NotTo(HaveOccurred())
		})
	})
})
