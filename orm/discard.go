package orm

type Discard struct{}

var _ Collection = (*Discard)(nil)
var _ ColumnScanner = (*Discard)(nil)

func (d Discard) NewModel(_ DB) ColumnScanner {
	return d
}

func (Discard) AddModel(_ DB, _ ColumnScanner) error {
	return nil
}

func (Discard) ScanColumn(colIdx int, colName string, b []byte) error {
	return nil
}
