package orm

import "strconv"

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
	joins   []string
	wheres  []string
	order   string
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

func (s *Select) Join(join string) *Select {
	s.joins = append(s.joins, join)
	return s
}

func (s *Select) Order(order string) *Select {
	s.order = order
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

	for i, column := range s.columns {
		if err := rel.AddRelation(column); err == nil {
			s.columns = append(s.columns[:i], s.columns[i+1:]...)
		}
	}

	s.tables = append(s.tables, rel.Model.Table.Name)
	if s.columns != nil {
		for name, model := range rel.HasOne {
			s.tables = append(s.tables, model.Table.Name)
			s.columns = model.Columns(s.columns, name+"__")
		}
	}

	err = s.db.QueryRelation(rel, s, rel)
	if err != nil {
		s.setErr(err)
		return s
	}

	for _, model := range rel.HasMany {
		err := NewSelect(s.db).Find(model).Err()
		if err != nil {
			s.setErr(err)
			return s
		}
	}

	return s
}

func (s *Select) AppendQuery(b []byte, params ...interface{}) ([]byte, error) {
	var err error

	f := NewFormatter(params)

	b = append(b, "SELECT "...)
	if s.columns == nil {
		b = append(b, " * "...)
	} else {
		b = appendSlice(b, s.columns)
	}

	b = append(b, " FROM "...)
	b = appendSlice(b, s.tables)

	b = appendSlice(b, s.joins)

	if s.order != "" {
		b = append(b, " ORDER BY "...)
		b, err = f.Append(b, s.order)
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

func appendSlice(b []byte, ss []string) []byte {
	for i, s := range ss {
		b = append(b, s...)
		if i != len(ss)-1 {
			b = append(b, ", "...)
		}
	}
	return b
}
