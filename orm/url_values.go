package orm

import (
	"net/url"
	"strconv"
	"time"

	"github.com/go-pg/pg/types"
)

type URLValues map[string][]string

func (v URLValues) Has(name string) bool {
	_, ok := v[name]
	return ok
}

func (v URLValues) SetDefault(name string, values ...string) {
	if !v.Has(name) {
		v[name] = values
	}
}

func (v URLValues) Strings(name string) []string {
	return v[name]
}

func (v URLValues) String(name string) string {
	values := v.Strings(name)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func (v URLValues) Int(name string) (int, error) {
	return strconv.Atoi(v.String(name))
}

func (v URLValues) MaybeInt(name string) int {
	n, _ := v.Int(name)
	return n
}

func (v URLValues) Int64(name string) (int64, error) {
	return strconv.ParseInt(v.String(name), 10, 64)
}

func (v URLValues) MaybeInt64(name string) int64 {
	n, _ := v.Int64(name)
	return n
}

func (v URLValues) Time(name string) (time.Time, error) {
	n, err := v.Int64(name)
	if err == nil {
		return time.Unix(n, 0), nil
	}
	return types.ParseTimeString(v.String(name))
}

func (v URLValues) MaybeTime(name string) time.Time {
	tm, _ := v.Time(name)
	return tm
}

func (v URLValues) Duration(name string) (time.Duration, error) {
	return time.ParseDuration(v.String(name))
}

func (v URLValues) MaybeDuration(name string) time.Duration {
	dur, _ := v.Duration(name)
	return dur
}

func (v URLValues) Pager() *Pager {
	return NewPager(url.Values(v))
}
