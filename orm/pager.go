package orm

import (
	"net/url"
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

func NewPager(values url.Values) *Pager {
	p := new(Pager)
	p.SetURLValues(values)
	return p
}

func (p *Pager) SetURLValues(urlValues url.Values) {
	values := URLValues(urlValues)

	if values.Has("limit") {
		limit, err := values.Int("limit")
		if err != nil {
			p.stickyErr = err
			return
		}
		p.Limit = int(limit)
	}

	if values.Has("page") {
		page, err := values.Int("page")
		if err != nil {
			p.stickyErr = err
			return
		}
		p.SetPage(int(page))
	}
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

func (p *Pager) Paginate(q *Query) (*Query, error) {
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
func Pagination(values url.Values) func(*Query) (*Query, error) {
	return NewPager(values).Paginate
}
