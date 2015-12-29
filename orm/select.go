package orm

import (
	"strconv"

	"gopkg.in/pg.v3/types"
)

type result interface {
	Affected() int
}

type dber interface {
	//Query(interface{}, string, ...interface{}) (result, error)
	QueryRelation(interface{}, interface{}, ...interface{}) error
}

type Select struct {
	db dber

	tables  []string
	columns []string
	fields  []string
	joins   []string
	wheres  []string
	orders  []string
	limit   int
	offset  int

	err error
}

func NewSelect(db dber) *Select {
	return &Select{db: db}
}

func (s *Select) setErr(err error) {
	if s.err == nil {
		s.err = err
	}
}

func (s *Select) Err() error {
	return s.err
}

func (s *Select) Select(columns ...string) *Select {
	s.columns = append(s.columns, columns...)
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
		s.err = err
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

func (s *Select) First(dst interface{}) *Select {
	return s.Order("?PK").Limit(1).Find(dst)
}

func (s *Select) Find(dst interface{}) *Select {
	rel, err := NewRelation(dst)
	if err != nil {
		s.setErr(err)
		return s
	}

	for i := 0; i < len(s.columns); {
		if err := rel.AddRelation(s.columns[i]); err == nil {
			s.columns = append(s.columns[:i], s.columns[i+1:]...)
			continue
		}
		i++
	}

	for _, field := range s.fields {
		rel.AddRelation(field)
	}

	s.tables = append(s.tables, rel.Model.Table.Name)
	if s.columns != nil {
		for _, join := range rel.Joins {
			if !join.TableRelation.Many {
				s = join.JoinOne(s)
			}
		}
	}

	err = s.db.QueryRelation(rel, s, rel)
	if err != nil {
		s.setErr(err)
		return s
	}

	for _, join := range rel.Joins {
		if join.TableRelation.Many {
			err := join.JoinMany(s.db, rel.Model.Value())
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
		b, err = appendField(f, b, s.columns...)
		if err != nil {
			return nil, err
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
		b, err = f.AppendBytes(b, types.AppendField(nil, field))
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
