package orm

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

func (m Discard) ScanColumn(colIdx int, colName string, b []byte) error {
	return nil
}
