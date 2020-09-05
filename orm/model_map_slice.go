package orm

type mapSliceModel struct {
	mapModel
	slice *[]map[string]interface{}
}

var _ Model = (*mapSliceModel)(nil)

func newMapSliceModel(ptr *[]map[string]interface{}) *mapSliceModel {
	return &mapSliceModel{
		slice: ptr,
	}
}

func (m *mapSliceModel) NextColumnScanner() ColumnScanner {
	m.mapModel.m = make(map[string]interface{})
	*m.slice = append(*m.slice, m.mapModel.m)
	return m
}

func (mapSliceModel) useQueryOne() {} //nolint:unused
