package pg

import (
	"fmt"
	"reflect"
	"strings"
)

func Where(s ...Q) Q {
	var b []byte
	for _, ss := range s {
		if ss == "" {
			continue
		}
		if b != nil {
			b = append(b, " AND "...)
		}
		b = append(b, ss...)
	}
	if b == nil {
		return Q("1 = 1")
	}
	return Q(b)
}

var defaultModel = model{}

type model struct {
	v interface{}
}

func (m model) AppendColumn(b []byte, v reflect.Value, name string) ([]byte, error) {
	v = m.value(v, false)

	fields := structs.Fields(v.Type()).Table
	if field, ok := fields[name]; ok {
		return field.AppendValue(b, v, true), nil
	}

	v = v.Addr()
	methods := structs.Methods(v.Type())
	if method, ok := methods[name]; ok {
		return method.AppendValue(b, v, true), nil
	}

	return nil, errorf("pg: cannot map %q on %s", name, v.Type())
}

func (m model) Fields(v reflect.Value) fields {
	return structs.Fields(m.value(v, false).Type())
}

func (m model) ColumnLoader(v reflect.Value) ColumnLoader {
	return newStructLoader(m.value(v, true))
}

func valueByNames(v reflect.Value, names []string, save bool) reflect.Value {
	for _, name := range names {
		v = v.FieldByName(name)
		if v.Kind() == reflect.Ptr {
			if v.IsNil() {
				if save {
					v.Set(reflect.New(v.Type().Elem()))
				} else {
					v = reflect.New(v.Type().Elem())
				}
			}
			v = v.Elem()
		}
	}
	return v
}

func (m model) value(base reflect.Value, save bool) reflect.Value {
	switch v := m.v.(type) {
	case nil:
		return base
	case []string:
		return valueByNames(base, v, save)
	case reflect.Value:
		return v
	default:
		panic("not reached")
	}
}

type Model struct {
	v    reflect.Value
	coll *sliceCollection

	models map[string]model
}

func NewModel(vi interface{}, name string) *Model {
	v := reflect.ValueOf(vi)
	if !v.IsValid() {
		panic(errorf("pg: Decode(nil)"))
	}
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	var coll *sliceCollection
	switch v.Kind() {
	case reflect.Struct:
		// ok
	default:
		var err error
		coll, err = newCollectionValue(v)
		if err != nil {
			panic(err)
		}
		v = reflect.Zero(v.Type().Elem())
	}

	return &Model{
		v:    v,
		coll: coll,

		models: map[string]model{
			name: defaultModel,
		},
	}
}

func (m *Model) HasOne(vi interface{}, name string) *Model {
	if _, ok := m.models[name]; ok {
		panic(fmt.Sprintf("model %s is already registered", name))
	}

	switch field := vi.(type) {
	case string:
		m.models[name] = model{
			v: strings.Split(field, "."),
		}
	default:
		m.models[name] = model{
			v: reflect.ValueOf(vi).Elem(),
		}
	}
	return m
}

func (m *Model) NewRecord() interface{} {
	m.v = m.coll.newValue()
	return m
}

func splitColumn(s string) (string, string) {
	parts := strings.SplitN(s, "__", 2)
	if len(parts) != 2 {
		return "", s
	}
	return parts[0], parts[1]
}

func (m *Model) appendName(b []byte, colName string) ([]byte, error) {
	switch colName {
	case "Fields":
		return m.appendFields(b), nil
	case "Values":
		return m.appendValues(b), nil
	case "Columns":
		return m.appendColumns(b), nil
	}

	modelName, fieldName := splitColumn(colName)
	mod, ok := m.models[modelName]
	if ok {
		return mod.AppendColumn(b, m.v, fieldName)
	}

	return defaultModel.AppendColumn(b, m.v, colName)
}

func (m *Model) LoadColumn(colIdx int, colName string, b []byte) error {
	modelName, colName := splitColumn(colName)
	mod, ok := m.models[modelName]
	if !ok {
		return errorf("pg: can't find model %q", modelName)
	}
	return mod.ColumnLoader(m.v).LoadColumn(colIdx, colName, b)
}

func (m *Model) appendColumns(b []byte) []byte {
	start := len(b)
	for tableAlias, mod := range m.models {
		for _, f := range mod.Fields(m.v).List {
			b = append(b, tableAlias...)
			b = append(b, '.')
			b = append(b, f.Name...)
			b = append(b, " AS "...)
			b = append(b, tableAlias...)
			b = append(b, '_', '_')
			b = append(b, f.Name...)
			b = append(b, ',', ' ')
		}
	}
	if len(b) != start {
		b = b[:len(b)-2]
	}
	return b
}

func (m *Model) appendFields(b []byte) []byte {
	start := len(b)
	for _, f := range defaultModel.Fields(m.v).List {
		if f.Is(nullEmpty) && f.IsEmpty(m.v) {
			continue
		}
		b = append(b, f.Name...)
		b = append(b, ',', ' ')
	}
	if len(b) != start {
		b = b[:len(b)-2]
	}
	return b
}

func (m *Model) appendValues(b []byte) []byte {
	start := len(b)
	for _, f := range defaultModel.Fields(m.v).List {
		if f.Is(nullEmpty) && f.IsEmpty(m.v) {
			continue
		}
		b = f.AppendValue(b, m.v, true)
		b = append(b, ',', ' ')
	}
	if len(b) != start {
		b = b[:len(b)-2]
	}
	return b
}
