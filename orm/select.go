package orm

import (
	"reflect"
	"strconv"

	"gopkg.in/pg.v3/types"
)

type dber interface {
	Exec(q interface{}, params ...interface{}) (types.Result, error)
	ExecOne(q interface{}, params ...interface{}) (types.Result, error)
	Query(coll, query interface{}, params ...interface{}) (types.Result, error)
	QueryOne(model, query interface{}, params ...interface{}) (types.Result, error)
}

type Select struct {
	db    dber
	model *Model
	err   error

	tables  []string
	columns []types.ValueAppender
	fields  []string
	joins   []string
	wheres  []string
	orders  []string
	limit   int
	offset  int
}

func NewSelect(db dber) *Select {
	return &Select{
		db: db,
	}
}

func (s *Select) Copy() *Select {
	return s
}

func (s *Select) setErr(err error) {
	if s.err == nil {
		s.err = err
	}
}

func (s *Select) Err() error {
	return s.err
}

func (s *Select) Select(columns ...interface{}) *Select {
	for _, column := range columns {
		v, ok := column.(types.ValueAppender)
		if !ok {
			v = types.F(column.(string))
		}
		s.columns = append(s.columns, v)
	}
	return s
}

func (s *Select) Table(name string) *Select {
	s.tables = append(s.tables, name)
	return s
}

func (s *Select) Where(where string, params ...interface{}) *Select {
	f := NewFormatter(params)
	b, err := f.Append(nil, where)
	if err != nil {
		s.setErr(err)
	} else {
		s.wheres = append(s.wheres, string(b))
	}

	for _, param := range params {
		if f, ok := param.(types.F); ok {
			s.fields = append(s.fields, string(f))
		}
	}

	return s
}

func (s *Select) Join(join string) *Select {
	s.joins = append(s.joins, join)
	return s
}

func (s *Select) Order(order string) *Select {
	s.orders = append(s.orders, order)
	return s
}

func (s *Select) Limit(n int) *Select {
	s.limit = n
	return s
}

func (s *Select) Offset(n int) *Select {
	s.offset = n
	return s
}

func (s *Select) Model(v interface{}) *Select {
	model, ok := v.(*Model)
	if !ok {
		var err error
		model, err = NewModel(v)
		if err != nil {
			s.setErr(err)
			return s
		}
	}
	s.model = model
	return s
}

func (s *Select) Count(count *int) *Select {
	s = s.Copy()

	s.columns = []types.ValueAppender{types.Q("COUNT(*)")}
	s.orders = nil
	s.limit = 0
	s.offset = 0

	s.tables = append(s.tables, s.model.Table.Name)
	for i := 0; i < len(s.columns); {
		b := s.columns[i].AppendValue(nil, true)
		if err := s.model.Join(string(b)); err == nil {
			s.columns = append(s.columns[:i], s.columns[i+1:]...)
			continue
		}
		i++
	}

	for _, field := range s.fields {
		s.model.Join(field + "._")
	}

	for _, join := range s.model.Joins {
		if !join.Relation.Many {
			s = join.JoinOne(s)
		}
	}

	_, err := s.db.Query(Scan(count), s, s.model)
	if err != nil {
		s.setErr(err)
		return s
	}

	return s
}

func (s *Select) First(dst interface{}) *Select {
	return s.Order("?PK").Limit(1).Find(dst)
}

func (s *Select) Last(dst interface{}) *Select {
	return s.Order("?PK DESC").Limit(1).Find(dst)
}

func (s *Select) Find(dst interface{}) *Select {
	var err error

	s.Model(dst)

	s.tables = append(s.tables, s.model.Table.Name)
	for i := 0; i < len(s.columns); {
		b := s.columns[i].AppendValue(nil, false)
		if err := s.model.Join(string(b)); err == nil {
			s.columns = append(s.columns[:i], s.columns[i+1:]...)
			continue
		}
		i++
	}

	for _, field := range s.fields {
		s.model.Join(field + "._")
	}

	for _, join := range s.model.Joins {
		if !join.Relation.Many {
			s = join.JoinOne(s)
		}
	}

	if s.model.Kind() == reflect.Struct {
		_, err = s.db.QueryOne(s.model, s, s.model)
	} else {
		_, err = s.db.Query(s.model, s, s.model)
	}
	if err != nil {
		s.setErr(err)
		return s
	}

	for _, join := range s.model.Joins {
		if join.Relation.Many {
			err := join.JoinMany(s.db, s.model.Value())
			if err != nil {
				s.setErr(err)
				return s
			}
		}
	}

	return s
}

func (s *Select) AppendQuery(b []byte, params ...interface{}) ([]byte, error) {
	var err error

	f := NewFormatter(params)

	b = append(b, "SELECT "...)
	if s.columns == nil {
		b = append(b, '*')
	} else {
		for i, column := range s.columns {
			b, err = f.AppendBytes(b, column.AppendValue(nil, true))
			if err != nil {
				return nil, err
			}
			if i != len(s.columns)-1 {
				b = append(b, ',', ' ')
			}
		}
	}

	b = append(b, " FROM "...)
	b, err = appendField(f, b, s.tables...)
	if err != nil {
		return nil, err
	}

	b, err = appendString(f, b, "", s.joins...)
	if err != nil {
		return nil, err
	}

	if s.wheres != nil {
		b = append(b, " WHERE "...)
		b, err = appendString(f, b, " AND ", s.wheres...)
		if err != nil {
			return nil, err
		}
	}

	if s.orders != nil {
		b = append(b, " ORDER BY "...)
		b, err = appendString(f, b, ", ", s.orders...)
		if err != nil {
			return b, err
		}
	}

	if s.limit != 0 {
		b = append(b, " LIMIT "...)
		b = strconv.AppendInt(b, int64(s.limit), 10)
	}

	if s.offset != 0 {
		b = append(b, " OFFSET "...)
		b = strconv.AppendInt(b, int64(s.offset), 10)
	}

	return b, nil
}

func appendField(f *Formatter, b []byte, ss ...string) ([]byte, error) {
	var err error
	for i, field := range ss {
		b, err = f.AppendBytes(b, types.AppendField(nil, field, true))
		if err != nil {
			return nil, err
		}

		if i != len(ss)-1 {
			b = append(b, ", "...)
		}
	}
	return b, nil
}

func appendString(f *Formatter, b []byte, sep string, ss ...string) ([]byte, error) {
	var err error
	for i, s := range ss {
		b, err = f.Append(b, s)
		if err != nil {
			return nil, err
		}

		if i != len(ss)-1 {
			b = append(b, sep...)
		}
	}
	return b, nil
}
