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

func (v URLValues) Bool(name string) (bool, error) {
	if !v.Has(name) {
		return false, nil
	}
	s := v.String(name)
	if s == "" {
		return true, nil
	}
	return strconv.ParseBool(s)
}

func (v URLValues) MaybeBool(name string) bool {
	flag, _ := v.Bool(name)
	return flag
}

func (v URLValues) Int(name string) (int, error) {
	s := v.String(name)
	if s == "" {
		return 0, nil
	}
	return strconv.Atoi(s)
}

func (v URLValues) MaybeInt(name string) int {
	n, _ := v.Int(name)
	return n
}

func (v URLValues) Int64(name string) (int64, error) {
	s := v.String(name)
	if s == "" {
		return 0, nil
	}
	return strconv.ParseInt(s, 10, 64)
}

func (v URLValues) MaybeInt64(name string) int64 {
	n, _ := v.Int64(name)
	return n
}

func (v URLValues) Time(name string) (time.Time, error) {
	s := v.String(name)
	if s == "" {
		return time.Time{}, nil
	}

	n, err := strconv.ParseInt(s, 10, 64)
	if err == nil {
		return time.Unix(n, 0), nil
	}
	return types.ParseTimeString(s)
}

func (v URLValues) MaybeTime(name string) time.Time {
	tm, _ := v.Time(name)
	return tm
}

func (v URLValues) Duration(name string) (time.Duration, error) {
	s := v.String(name)
	if s == "" {
		return 0, nil
	}
	return time.ParseDuration(s)
}

func (v URLValues) MaybeDuration(name string) time.Duration {
	dur, _ := v.Duration(name)
	return dur
}

func (v URLValues) Pager() *Pager {
	return NewPager(url.Values(v))
}
