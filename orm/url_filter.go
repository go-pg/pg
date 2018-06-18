package orm

import (
	"net/url"
	"strings"

	"github.com/go-pg/pg/types"
)

// URLFilter is used with Query.Apply to add WHERE clauses from the URL values:
//   - ?foo=bar - Where(`"foo" = 'bar'`)
//   - ?foo=hello&foo=world - Where(`"foo" IN ('hello','world')`)
//   - ?foo__exclude=bar - Where(`"foo" != 'bar'`)
//   - ?foo__ieq=bar - Where(`"foo" ILIKE 'bar'`)
//   - ?foo__match=bar - Where(`"foo" SIMILAR TO 'bar'`)
//   - ?foo__gt=42 - Where(`"foo" > 42`)
//   - ?foo__gte=42 - Where(`"foo" >= 42`)
//   - ?foo__lt=42 - Where(`"foo" < 42`)
//   - ?foo__lte=42 - Where(`"foo" <= 42`)
type URLFilter struct {
	values  URLValues
	allowed map[string]struct{}
}

func NewURLFilter(values url.Values) *URLFilter {
	return &URLFilter{
		values: URLValues(values),
	}
}

// Values returns URL values.
func (f *URLFilter) Values() URLValues {
	return f.values
}

func (f *URLFilter) Allow(filter string) {
	if f.allowed == nil {
		f.allowed = make(map[string]struct{})
	}
	f.allowed[filter] = struct{}{}
}

func (f *URLFilter) isAllowed(filter string) bool {
	if len(f.allowed) == 0 {
		return true
	}
	_, ok := f.allowed[filter]
	return ok
}

func (f *URLFilter) Filters(q *Query) (*Query, error) {
	if f == nil {
		return q, nil
	}

	for filter, values := range f.values {
		if !f.isAllowed(filter) {
			continue
		}

		var operation string
		if i := strings.Index(filter, "__"); i != -1 {
			filter, operation = filter[:i], filter[i+2:]
		}

		if q.model.Table().HasField(filter) {
			q = addOperator(q, filter, operation, values)
		}
	}
	return q, nil
}

// URLFilters is a shortcut for NewURLFilter(urlValues).Filters.
func URLFilters(urlValues url.Values) func(*Query) (*Query, error) {
	return NewURLFilter(urlValues).Filters
}

func addOperator(q *Query, field, operator string, values []string) *Query {
	switch operator {
	case "gt":
		q = forEachValue(q, field, values, "? > ?")
	case "gte":
		q = forEachValue(q, field, values, "? >= ?")
	case "lt":
		q = forEachValue(q, field, values, "? < ?")
	case "lte":
		q = forEachValue(q, field, values, "? <= ?")
	case "ieq":
		q = forEachValue(q, field, values, "? ILIKE ?")
	case "match":
		q = forEachValue(q, field, values, "? SIMILAR TO ?")
	case "exclude":
		q = forAllValues(q, field, values, "? != ?", "? NOT IN (?)")
	case "", "include":
		q = forAllValues(q, field, values, "? = ?", "? IN (?)")
	}
	return q
}

func forEachValue(q *Query, field string, values []string, queryTemplate string) *Query {
	for _, value := range values {
		q = q.Where(queryTemplate, types.F(field), value)
	}
	return q
}

func forAllValues(q *Query, field string, values []string, queryTemplate, queryArrayTemplate string) *Query {
	if len(values) > 1 {
		q = q.Where(queryArrayTemplate, types.F(field), types.InSlice(values))
	} else {
		q = q.Where(queryTemplate, types.F(field), values[0])
	}
	return q
}
