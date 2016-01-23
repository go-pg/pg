package orm

type Discard struct{}

var _ Collection = (*Discard)(nil)
var _ ColumnScanner = (*Discard)(nil)

func (d Discard) NextModel() interface{} {
	return d
}

func (Discard) ScanColumn(colIdx int, colName string, b []byte) error {
	return nil
}
