package orm

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type InsertTest struct{}

var _ = Describe("Insert", func() {
	It("supports multiple OnConflict", func() {
		q := NewQuery(nil, &InsertTest{}).
			OnConflict("(unq1) DO UPDATE").
			Set("count1 = count1 + 1").
			Where("cond1 IS TRUE").
			OnConflict("(unq2) DO UPDATE").
			Set("count2 = count2 + 1").
			Set("count3 = count3 + 1").
			Where("cond2 IS TRUE").
			Where("cond3 IS TRUE")

		b, err := insertQuery{Query: q}.AppendQuery(nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(b)).To(Equal(`INSERT INTO "insert_tests" AS "insert_test" () VALUES () ON CONFLICT (unq1) DO UPDATE SET count1 = count1 + 1 WHERE (cond1 IS TRUE) ON CONFLICT (unq2) DO UPDATE SET count2 = count2 + 1, count3 = count3 + 1 WHERE (cond2 IS TRUE) AND (cond3 IS TRUE)`))
	})
})
