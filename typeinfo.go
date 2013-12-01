package pg

import (
	"reflect"
	"strings"
	"sync"
)

var (
	structs = newStructCache()
)

func isUpper(c byte) bool {
	return c >= 'A' && c <= 'Z'
}

func isLower(c byte) bool {
	return !isUpper(c)
}

func toLower(c byte) byte {
	return c + 32
}

func formatColumnName(s string) string {
	b := []byte(s)
	r := make([]byte, 0, len(b))
	for i := 0; i < len(b); i++ {
		c := b[i]
		if isUpper(c) {
			if i-1 > 0 && i+1 < len(b) && (isLower(b[i-1]) || isLower(b[i+1])) {
				r = append(r, '_', toLower(c))
			} else {
				r = append(r, toLower(c))
			}
		} else {
			r = append(r, c)
		}
	}
	return string(r)
}

type structCache struct {
	l sync.RWMutex
	m map[reflect.Type]map[string][]int
}

func newStructCache() *structCache {
	return &structCache{
		m: make(map[reflect.Type]map[string][]int),
	}
}

func (c *structCache) Indexes(typ reflect.Type) map[string][]int {
	c.l.RLock()
	indxs, ok := c.m[typ]
	c.l.RUnlock()
	if ok {
		return indxs
	}

	numField := typ.NumField()
	indxs = make(map[string][]int, numField)
	for i := 0; i < numField; i++ {
		f := typ.Field(i)
		if f.PkgPath != "" {
			continue
		}

		tokens := strings.Split(f.Tag.Get("pg"), ",")
		name := tokens[0]
		if name == "-" {
			continue
		}
		if name == "" {
			name = formatColumnName(f.Name)
		}
		indxs[name] = f.Index
	}

	c.l.Lock()
	c.m[typ] = indxs
	c.l.Unlock()

	return indxs
}
