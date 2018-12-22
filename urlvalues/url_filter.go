package urlvalues

import (
	"strings"

	"github.com/go-pg/pg"
	"github.com/go-pg/pg/internal"
	"github.com/go-pg/pg/orm"
	"github.com/go-pg/pg/types"
)

// URLFilter is used with Query.Apply to add WHERE clauses from the URL values:
//   - ?foo=bar - Where(`"foo" = 'bar'`)
//   - ?foo=hello&foo=world - Where(`"foo" IN ('hello','world')`)
//   - ?foo__neq=bar - Where(`"foo" != 'bar'`)
//   - ?foo__exclude=bar - Where(`"foo" != 'bar'`)
//   - ?foo__gt=42 - Where(`"foo" > 42`)
//   - ?foo__gte=42 - Where(`"foo" >= 42`)
//   - ?foo__lt=42 - Where(`"foo" < 42`)
//   - ?foo__lte=42 - Where(`"foo" <= 42`)
//   - ?foo__ieq=bar - Where(`"foo" ILIKE 'bar'`)
//   - ?foo__match=bar - Where(`"foo" SIMILAR TO 'bar'`)
type Filter struct {
	values  Values
	allowed map[string]struct{}
}

func NewFilter(values Values) *Filter {
	return &Filter{
		values: values,
	}
}

// Values returns URL values.
func (f *Filter) Values() Values {
	return f.values
}

func (f *Filter) Allow(filters ...string) {
	if f.allowed == nil {
		f.allowed = make(map[string]struct{})
	}
	for _, filter := range filters {
		f.allowed[filter] = struct{}{}
	}
}

func (f *Filter) isAllowed(filter string) bool {
	if len(f.allowed) == 0 {
		return true
	}
	_, ok := f.allowed[filter]
	return ok
}

func (f *Filter) Filters(q *orm.Query) (*orm.Query, error) {
	if f == nil {
		return q, nil
	}

	var b []byte
	for filter, values := range f.values {
		if strings.HasSuffix(filter, "[]") {
			filter = filter[:len(filter)-2]
		}

		if !f.isAllowed(filter) {
			continue
		}

		var operation string
		if ind := strings.Index(filter, "__"); ind != -1 {
			filter, operation = filter[:ind], filter[ind+2:]
		}

		if q.GetModel().Table().HasField(filter) {
			if b != nil {
				b = append(b, " AND "...)
			}
			b = addOperator(b, filter, operation, values)
		}
	}

	if len(b) > 0 {
		q = q.Where(internal.BytesToString(b))
	}
	return q, nil
}

// Filters is a shortcut for NewFilter(urlValues).Filters.
func Filters(values Values) func(*orm.Query) (*orm.Query, error) {
	return NewFilter(values).Filters
}

func addOperator(b []byte, field, op string, values []string) []byte {
	switch op {
	case "", "include":
		b = forAllValues(b, field, values, " = ", " IN ")
	case "exclude", "neq":
		b = forAllValues(b, field, values, " != ", " NOT IN ")
	case "gt":
		b = forEachValue(b, field, values, " > ")
	case "gte":
		b = forEachValue(b, field, values, " >= ")
	case "lt":
		b = forEachValue(b, field, values, " < ")
	case "lte":
		b = forEachValue(b, field, values, " <= ")
	case "ieq":
		b = forEachValue(b, field, values, " ILIKE ")
	case "match":
		b = forEachValue(b, field, values, " SIMILAR TO ")
	}
	return b
}

func forEachValue(
	b []byte, field string, values []string, opValue string,
) []byte {
	for _, value := range values {
		b = types.AppendField(b, field, 1)
		b = append(b, opValue...)
		b = types.AppendString(b, value, 1)
	}
	return b
}

func forAllValues(
	b []byte, field string, values []string, singleOpValue, multiOpValue string,
) []byte {
	if len(values) <= 1 {
		return forEachValue(b, field, values, singleOpValue)
	}

	b = types.AppendField(b, field, 1)
	b = append(b, multiOpValue...)
	b = append(b, '(')
	b = pg.Strings(values).AppendValue(b, 1)
	b = append(b, ')')
	return b
}
