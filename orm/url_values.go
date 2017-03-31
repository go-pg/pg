package orm

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/go-pg/pg/types"
)

// URLFilters is used with Query.Apply to add WHERE clauses from the URL values:
//   - ?foo=bar - Where(`"foo" = 'bar'`)
//   - ?foo=hello&foo=world - Where(`"foo" IN ('hello','world')`)
//   - ?foo__exclude=bar - Where(`"foo" != 'bar'`)
//   - ?foo__ieq=bar - Where(`"foo" ILIKE 'bar'`)
//   - ?foo__match=bar - Where(`"foo" SIMILAR TO 'bar'`)
//   - ?foo__gt=42 - Where(`"foo" > 42`)
//   - ?foo__gte=42 - Where(`"foo" >= 42`)
//   - ?foo__lt=42 - Where(`"foo" < 42`)
//   - ?foo__lte=42 - Where(`"foo" <= 42`)
func URLFilters(urlValues url.Values) func(*Query) (*Query, error) {
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
		return q, nil
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
	case "ieq":
		q = forEachValue(q, fieldName, values, "? ILIKE ?")
	case "match":
		q = forEachValue(q, fieldName, values, "? SIMILAR TO ?")
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

type Pager struct {
	limit int
	page  int

	stickyErr error
}

func NewPager(values url.Values, defaultLimit int) *Pager {
	limit, err := intParam(values, "limit")
	if err != nil {
		return &Pager{stickyErr: err}
	}
	if limit <= 0 {
		limit = defaultLimit
	}

	page, err := intParam(values, "page")
	if err != nil {
		return &Pager{stickyErr: err}
	}

	return &Pager{
		limit: limit,
		page:  page,
	}
}

func (p *Pager) Limit() int {
	return p.limit
}

func (p *Pager) Page() int {
	return p.page
}

func (p *Pager) Offset() int {
	if p.page > 0 {
		return (p.page - 1) * p.limit
	}
	return 0
}

func (p *Pager) Paginate(q *Query) (*Query, error) {
	const maxLimit = 1000
	const maxOffset = 1000000

	if p.stickyErr != nil {
		return nil, p.stickyErr
	}

	if p.limit > maxLimit {
		return nil, fmt.Errorf("limit=%d is bigger than %d", p.limit, maxLimit)
	}
	q = q.Limit(p.limit)

	if offset := p.Offset(); offset > 0 {
		if offset > maxOffset {
			return nil, fmt.Errorf("offset=%d is bigger than %d", offset, maxOffset)
		}
		q = q.Offset(offset)
	}

	return q, nil
}

// Pagination is used with Query.Apply to set LIMIT and OFFSET from the URL values:
//   - ?limit=10 - sets q.Limit(10), max limit is 1000.
//   - ?page=5 - sets q.Offset((page - 1) * limit), max offset is 1000000.
func Pagination(values url.Values, defaultLimit int) func(*Query) (*Query, error) {
	return NewPager(values, defaultLimit).Paginate
}

func intParam(urlValues url.Values, paramName string) (int, error) {
	values, ok := urlValues[paramName]
	if !ok {
		return 0, nil
	}

	value, err := strconv.Atoi(values[0])
	if err != nil {
		return 0, fmt.Errorf("param=%s value=%s is invalid: %s", paramName, values[0], err)
	}

	return value, nil
}
