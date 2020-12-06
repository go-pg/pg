package types

import (
	"database/sql/driver"

	"github.com/pkg/errors"
)

type BoundType byte

const (
	// named awkwardly to avoid collisions without checking
	RBoundInclusive = BoundType('i')
	RBoundExclusive = BoundType('e')
	RBoundUnbounded = BoundType('U')
	RBoundEmpty     = BoundType('E')
)

func (bt BoundType) String() string {
	return string(bt)
}

// -----------------------------------------------------------------------------------------------------

type NullTimeRange struct {
	Lower     NullTime `json:"lower"`
	Upper     NullTime `json:"upper"`
	LowerType BoundType
	UpperType BoundType
}

func (r *NullTimeRange) Scan(src interface{}) (err error) {
	if src == nil {
		r.decodeText(nil)
		return
	}

	switch src := src.(type) {
	case string:
		return r.decodeText([]byte(src))
	case []byte:
		srcCopy := make([]byte, len(src))
		copy(srcCopy, src)
		return r.decodeText(srcCopy)
	}

	err = errors.Errorf("cannot scan %T", src)
	return
}

func (r NullTimeRange) Value() (value driver.Value, err error) {
	buf, err := r.encodeText(make([]byte, 0)) //, 32))
	if err != nil {
		return
	}
	if buf == nil {
		return
	}
	value = string(buf)
	return
}

func (r *NullTimeRange) decodeText(src []byte) error {
	*r = NullTimeRange{
		Lower:     NullTime{},
		Upper:     NullTime{},
		LowerType: RBoundEmpty,
		UpperType: RBoundEmpty,
	}
	if src == nil {
		return nil
	}

	utr, err := parseUntypedTextRange(string(src))
	if err != nil {
		return err
	}

	r.LowerType = utr.LowerType
	r.UpperType = utr.UpperType

	if r.LowerType == RBoundEmpty {
		return nil
	}

	if r.LowerType == RBoundInclusive || r.LowerType == RBoundExclusive {
		t, tsv, err := ParseTimeString(utr.Lower)
		if err != nil {
			return err
		}
		r.Lower.Time = t
		r.Lower.Special = tsv
	}

	if r.UpperType == RBoundInclusive || r.UpperType == RBoundExclusive {
		t, tsv, err := ParseTimeString(utr.Upper)
		if err != nil {
			return err
		}
		r.Upper.Time = t
		r.Upper.Special = tsv
	}

	return nil
}

func (r NullTimeRange) encodeText(buf []byte) ([]byte, error) {

	if (r.Lower.IsZero() && r.Lower.Special == TSVNone) || (r.Upper.IsZero() && r.Upper.Special == TSVNone) {
		return nil, nil
	}

	switch r.LowerType {
	case RBoundExclusive, RBoundUnbounded:
		buf = append(buf, '(')
	case RBoundInclusive:
		buf = append(buf, '[')
	case RBoundEmpty:
		return append(buf, "empty"...), nil
	default:
		return nil, errors.Errorf("unknown lower bound type %v", r.LowerType)
	}

	if r.LowerType != RBoundUnbounded {
		buf = r.Lower.forRange(buf)
		if buf == nil {
			return nil, errors.Errorf("lower cannot be null unless lowerType is unbounded")
		}
	}

	buf = append(buf, ',')

	if r.UpperType != RBoundUnbounded {
		buf = r.Upper.forRange(buf)
		if buf == nil {
			return nil, errors.Errorf("upper cannot be null unless upperType is unbounded")
		}
	}

	switch r.UpperType {
	case RBoundExclusive, RBoundUnbounded:
		buf = append(buf, ')')
	case RBoundInclusive:
		buf = append(buf, ']')
	default:
		return nil, errors.Errorf("unknown upper bound type %v", r.UpperType)
	}

	return buf, nil
}
