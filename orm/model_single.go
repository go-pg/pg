package orm

type singleModel struct {
	Model
}

var _ Collection = (*singleModel)(nil)

func NewSingleModel(mod interface{}) (Model, error) {
	model, ok := mod.(Model)
	if !ok {
		var err error
		model, err = NewModel(mod)
		if err != nil {
			return nil, err
		}
	}
	return &singleModel{
		Model: model,
	}, nil
}
