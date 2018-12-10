package urlvalues

import (
	"github.com/go-pg/pg/orm"
)

type Pager struct {
	Limit  int
	Offset int

	// Default max limit is 1000.
	MaxLimit int
	// Default max offset is 1000000.
	MaxOffset int

	stickyErr error
}

func NewPager(values Values) *Pager {
	p := new(Pager)
	p.stickyErr = p.FromValues(values)
	return p
}

func (p *Pager) FromURLValues(values Values) error {
	return p.FromValues(Values(values))
}

func (p *Pager) FromValues(values Values) error {
	limit, err := values.Int("limit")
	if err != nil {
		return err
	}
	p.Limit = int(limit)

	page, err := values.Int("page")
	if err != nil {
		return err
	}
	p.SetPage(int(page))

	return nil
}

func (p *Pager) maxLimit() int {
	if p.MaxLimit > 0 {
		return p.MaxLimit
	}
	return 1000
}

func (p *Pager) maxOffset() int {
	if p.MaxOffset > 0 {
		return p.MaxOffset
	}
	return 1000000
}

func (p *Pager) GetLimit() int {
	const defaultLimit = 100

	if p == nil {
		return defaultLimit
	}
	if p.Limit < 0 {
		return p.Limit
	}
	if p.Limit == 0 {
		return defaultLimit
	}
	if p.Limit > p.maxLimit() {
		return p.maxLimit()
	}
	return p.Limit
}

func (p *Pager) GetOffset() int {
	if p == nil {
		return 0
	}
	if p.Offset > p.maxOffset() {
		return p.maxOffset()
	}
	return p.Offset
}

func (p *Pager) SetPage(page int) {
	if page < 1 {
		page = 1
	}
	p.Offset = (page - 1) * p.GetLimit()
}

func (p *Pager) GetPage() int {
	return (p.GetOffset() / p.GetLimit()) + 1
}

func (p *Pager) Pagination(q *orm.Query) (*orm.Query, error) {
	if p == nil {
		return q, nil
	}
	if p.stickyErr != nil {
		return nil, p.stickyErr
	}

	q = q.Limit(p.GetLimit()).Offset(p.GetOffset())
	return q, nil
}

// Pagination is used with Query.Apply to set LIMIT and OFFSET from the URL values:
//   - ?limit=10 - sets q.Limit(10), max limit is 1000.
//   - ?page=5 - sets q.Offset((page - 1) * limit), max offset is 1000000.
func Pagination(values Values) func(*orm.Query) (*orm.Query, error) {
	return NewPager(values).Pagination
}
