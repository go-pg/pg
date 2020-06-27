package orm

type mapModel struct {
	Discard
	m map[string]interface{}
}

var _ Model = (*mapModel)(nil)

func newMapModel(m map[string]interface{}) *mapModel {
	return &mapModel{
		m: m,
	}
}
