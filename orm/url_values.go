package orm

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"gopkg.in/pg.v5/types"
)

func URLValues(urlValues url.Values) func(*Query) (*Query, error) {
	return func(q *Query) (*Query, error) {
		for fieldName, values := range urlValues {
			var operation string
			if i := strings.Index(fieldName, "__"); i != -1 {
				fieldName, operation = fieldName[:i], fieldName[i+2:]
			}

			if q.model.Table().HasField(fieldName) {
				q = addOperator(q, fieldName, operation, values)
			}
		}

		return setOrder(q, urlValues), nil
	}
}

func addOperator(q *Query, fieldName, operator string, values []string) *Query {
	switch operator {
	case "gt":
		q = forEachValue(q, fieldName, values, "? > ?")
	case "gte":
		q = forEachValue(q, fieldName, values, "? >= ?")
	case "lt":
		q = forEachValue(q, fieldName, values, "? < ?")
	case "lte":
		q = forEachValue(q, fieldName, values, "? <= ?")
	case "exclude":
		q = forAllValues(q, fieldName, values, "? != ?", "? NOT IN (?)")
	case "", "include":
		q = forAllValues(q, fieldName, values, "? = ?", "? IN (?)")
	}
	return q
}

func forEachValue(q *Query, fieldName string, values []string, queryTemplate string) *Query {
	for _, value := range values {
		q = q.Where(queryTemplate, types.F(fieldName), value)
	}
	return q
}

func forAllValues(q *Query, fieldName string, values []string, queryTemplate, queryArrayTemplate string) *Query {
	if len(values) > 1 {
		q = q.Where(queryArrayTemplate, types.F(fieldName), types.In(values))
	} else {
		q = q.Where(queryTemplate, types.F(fieldName), values[0])
	}
	return q
}

func setOrder(q *Query, urlValues url.Values) *Query {
	if orders, ok := urlValues["order"]; ok {
		for _, order := range orders {
			q = q.OrderExpr(order)
		}
	}
	return q
}

func Pager(urlValues url.Values) func(*Query) (*Query, error) {
	return func(q *Query) (*Query, error) {
		const pageSize = 100

		limit, err := intParam(urlValues, "limit")
		if err != nil {
			return nil, err
		}
		if limit < 1 {
			limit = pageSize
		}
		q = q.Limit(int(limit))

		page, err := intParam(urlValues, "page")
		if err != nil {
			return nil, err
		}
		if page > 0 {
			q = q.Offset((int(page) - 1) * int(limit))
		}

		return q, nil
	}
}

func intParam(urlValues url.Values, fieldName string) (int, error) {
	values, ok := urlValues[fieldName]
	if !ok {
		return 0, nil
	}

	value, err := strconv.Atoi(values[0])
	if err != nil {
		return 0, fmt.Errorf("%s is invalid: %s", fieldName, urlValues["limit"][0])
	}

	return value, nil
}
