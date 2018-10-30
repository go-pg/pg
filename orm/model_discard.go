package orm

import (
	"github.com/go-pg/pg/types"
)

type Discard struct {
	hookStubs
}

var _ Model = (*Discard)(nil)

func (Discard) Init() error {
	return nil
}

func (m Discard) NewModel() ColumnScanner {
	return m
}

func (m Discard) AddModel(ColumnScanner) error {
	return nil
}

func (m Discard) ScanColumn(colIdx int, colName string, rd types.Reader, n int) error {
	return nil
}
