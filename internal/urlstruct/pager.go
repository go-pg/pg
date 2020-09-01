package urlstruct

import (
	"context"
	"net/url"
)

type Pager struct {
	Limit  int `urlstruct:"-"`
	Offset int `urlstruct:"-"`

	// Default max limit is 1000.
	MaxLimit int `urlstruct:"-"`
	// Default max offset is 1000000.
	MaxOffset int `urlstruct:"-"`

	stickyErr error
}

func NewPager(values url.Values) *Pager {
	p := new(Pager)
	p.stickyErr = p.UnmarshalValues(context.TODO(), values)
	return p
}

var _ Unmarshaler = (*Pager)(nil)

func (p *Pager) UnmarshalValues(ctx context.Context, values url.Values) error {
	vs := Values(values)

	limit, err := vs.Int("limit")
	if err != nil {
		return err
	}
	p.Limit = limit

	page, err := vs.Int("page")
	if err != nil {
		return err
	}
	p.SetPage(page)

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
