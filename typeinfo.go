package pg

import (
	"reflect"
	"strings"
	"sync"
)

var (
	tinfoMap = newTypeInfoMap()
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

type typeInfoMap struct {
	l sync.RWMutex
	m map[reflect.Type]map[string][]int
}

func newTypeInfoMap() *typeInfoMap {
	return &typeInfoMap{
		m: make(map[reflect.Type]map[string][]int),
	}
}

func (m *typeInfoMap) Indexes(typ reflect.Type) map[string][]int {
	m.l.RLock()
	indxs, ok := m.m[typ]
	m.l.RUnlock()
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

	m.l.Lock()
	m.m[typ] = indxs
	m.l.Unlock()

	return indxs
}
