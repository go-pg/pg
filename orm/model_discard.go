package orm

type Discard struct {
	hookStubs
}

var _ Model = (*Discard)(nil)

func (Discard) Reset() error {
	return nil
}

func (d Discard) NewModel() ColumnScanner {
	return d
}

func (Discard) AddModel(_ ColumnScanner) error {
	return nil
}

func (Discard) ScanColumn(colIdx int, colName string, b []byte) error {
	return nil
}
