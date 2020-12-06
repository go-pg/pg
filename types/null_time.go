package types

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"time"
)

var jsonNull = []byte("null")

// -----------------------------------------------------------------------------------------------------

// NullTime is a time.Time wrapper that marshals zero time as JSON null and
// PostgreSQL NULL.
type NullTime struct {
	time.Time
	Special TimeSpecialValue
}

var (
	_ json.Marshaler   = (*NullTime)(nil)
	_ json.Unmarshaler = (*NullTime)(nil)
	_ sql.Scanner      = (*NullTime)(nil)
	_ ValueAppender    = (*NullTime)(nil)
)

func (tm NullTime) MarshalJSON() (b []byte, err error) {
	if (tm.Special == TSVInfinity) || (tm.Special == -TSVInfinity) {
		b = make([]byte, 0)
		b = append(b, `"`...)
		b = append(b, tm.Special.Bytes()...)
		b = append(b, `"`...)
		return
	}
	if tm.IsZero() {
		b = jsonNull
		return
	}
	b, err = tm.Time.MarshalJSON()
	return
}

func (tm *NullTime) UnmarshalJSON(b []byte) error {
	if bytes.Equal(b, jsonNull) {
		tm.Time = time.Time{}
		return nil
	}
	if bytes.Equal(b, TSVInfinity.Bytes()) {
		tm.Time = time.Time{}
		tm.Special = TSVInfinity
		return nil
	}
	if bytes.Equal(b, TSVNegativeInfinity.Bytes()) {
		tm.Time = time.Time{}
		tm.Special = -TSVInfinity
		return nil
	}
	return tm.Time.UnmarshalJSON(b)
}

func (tm NullTime) AppendValue(b []byte, flags int) ([]byte, error) {
	if tm.IsZero() {
		return AppendNull(b, flags), nil
	}
	return AppendTime(b, tm.Time, flags), nil
}

func (tm *NullTime) Scan(b interface{}) (err error) {
	tm.Special = TSVNone
	if b == nil {
		return nil
	}
	switch typedB := b.(type) {
	case string, []byte:
		tm.Time, tm.Special, err = ParseTime(typedB.([]byte))
		return
	case time.Time:
		tm.Time = typedB
		return nil
	}
	return
}

// Value implements the database/sql/driver Valuer interface.
func (tm NullTime) Value() (driver.Value, error) {
	if tm.Special != TSVNone {
		return tm.Special.String(), nil
	}
	return tm.Time, nil
}

func (tm NullTime) forRange(b []byte) []byte {
	switch tm.Special {
	case TSVInfinity:
		b = append(b, []byte("infinity")...)
	case TSVNegativeInfinity:
		b = append(b, []byte("-infinity")...)
	default:
		if tm.IsZero() {
			return nil
		}
		b = tm.Time.UTC().AppendFormat(b, timestamptzFormat)
	}
	return b
}
