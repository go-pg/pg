package types

import (
	"github.com/elliotcourant/gomonetary"
)

type Money float64

func (m *Money) Scan(b interface{}) error {
	if b == nil {
		*m = 0
		return nil
	}
	val, err := monetary.ParseDefault(string(b.([]byte)))
	*m = Money(val)
	return err
}
