package orm

type Discard struct{}

var _ Collection = (*Discard)(nil)
var _ ColumnScanner = (*Discard)(nil)

func (d Discard) NewModel() ColumnScanner {
	return d
}

func (Discard) AddModel(_ ColumnScanner) error {
	return nil
}

func (Discard) ScanColumn(colIdx int, colName string, b []byte) error {
	return nil
}

func (Discard) AfterQuery(DB) error {
	return nil
}

func (Discard) AfterSelect(DB) error {
	return nil
}

func (Discard) BeforeCreate(DB) error {
	return nil
}

func (Discard) AfterCreate(DB) error {
	return nil
}
