package types

type ValuesOp []*InOp

var _ ValueAppender = (*ValuesOp)(nil)

func (vs ValuesOp) AppendValue(b []byte, quote int) ([]byte, error) {
	var err error
	b = append(b, "VALUES "...)
	for _, v := range vs {
		b = append(b, '(')
		b, err = v.AppendValue(b, quote)
		if err != nil {
			return nil, err
		}
		b = append(b, "), "...)
	}
	if len(vs) > 0 {
		b = b[:len(b)-2]
	}

	return b, nil
}

// Values computes a row value or set of row values specified by value expressions.
func Values(slices []interface{}) ValuesOp {
	op := ValuesOp{}
	for _, slice := range slices {
		op = append(op, In(slice))
	}
	return op
}
