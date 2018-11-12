package types

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"time"
)

var jsonNull = []byte("null")

// NullTime is a time.Time wrapper that marshals zero time as JSON null and
// PostgreSQL NULL.
type NullTime struct {
	time.Time
}

var _ json.Marshaler = (*NullTime)(nil)
var _ json.Unmarshaler = (*NullTime)(nil)
var _ sql.Scanner = (*NullTime)(nil)
var _ ValueAppender = (*NullTime)(nil)

func (tm NullTime) MarshalJSON() ([]byte, error) {
	if tm.IsZero() {
		return jsonNull, nil
	}
	return tm.Time.MarshalJSON()
}

func (tm *NullTime) UnmarshalJSON(b []byte) error {
	if bytes.Equal(b, jsonNull) {
		tm.Time = time.Time{}
		return nil
	}
	return tm.Time.UnmarshalJSON(b)
}

func (tm NullTime) AppendValue(b []byte, quote int) []byte {
	if tm.IsZero() {
		return AppendNull(b, quote)
	}
	return AppendTime(b, tm.Time, quote)
}

func (tm *NullTime) Scan(b interface{}) error {
	if b == nil {
		tm.Time = time.Time{}
		return nil
	}
	newtm, err := ParseTime(b.([]byte))
	if err != nil {
		return err
	}
	tm.Time = newtm
	return nil
}
