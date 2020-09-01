package urlstruct

import (
	"context"
	"fmt"
	"net/url"
	"reflect"
	"sync"
)

var globalMap structInfoMap

func DescribeStruct(typ reflect.Type) *StructInfo {
	return globalMap.DescribeStruct(typ)
}

// Unmarshal unmarshals url values into the struct.
func Unmarshal(ctx context.Context, values url.Values, strct interface{}) error {
	d := newStructDecoder(reflect.ValueOf(strct))
	return d.Decode(ctx, values)
}

type structInfoMap struct {
	m sync.Map
}

func (m *structInfoMap) DescribeStruct(typ reflect.Type) *StructInfo {
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct {
		panic(fmt.Errorf("got %s, wanted %s", typ.Kind(), reflect.Struct))
	}

	if v, ok := m.m.Load(typ); ok {
		return v.(*StructInfo)
	}

	sinfo := newStructInfo(typ)
	if v, loaded := m.m.LoadOrStore(typ, sinfo); loaded {
		return v.(*StructInfo)
	}
	return sinfo
}
