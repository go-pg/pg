package types

import (
	"time"

	"github.com/go-pg/pg/v10/internal"
)

const (
	dateFormat         = "2006-01-02"
	timeFormat         = "15:04:05.999999999"
	timestampFormat    = "2006-01-02 15:04:05.999999999"
	timestamptzFormat  = "2006-01-02 15:04:05.999999999-07:00:00"
	timestamptzFormat2 = "2006-01-02 15:04:05.999999999-07:00"
	timestamptzFormat3 = "2006-01-02 15:04:05.999999999-07"
)

type TimeSpecialValue int8

// named awkwardly to avoid collisions without checking.
const (
	TSVNegativeInfinity TimeSpecialValue = iota - 1
	TSVNone
	TSVInfinity
	TSVEpoch
	TSVNow
	TSVToday
	TSVTomorrow
	TSVYesterday
	TSVAllBalls
)

func (im TimeSpecialValue) Bytes() []byte {
	return []byte(im.String())
}

func (im TimeSpecialValue) String() string {
	switch im {
	case TSVNone:
		return "none"
	case TSVInfinity:
		return "infinity"
	case TSVNegativeInfinity:
		return "-infinity"
	case TSVEpoch:
		return "epoch"
	case TSVNow:
		return "now"
	case TSVToday:
		return "today"
	case TSVTomorrow:
		return "tomorrow"
	case TSVYesterday:
		return "yesterday"
	case TSVAllBalls:
		return "allballs"
	default:
		return "invalid"
	}
}

func ParseTime(b []byte) (time.Time, TimeSpecialValue, error) {
	s := internal.BytesToString(b)
	return ParseTimeString(s)
}

func ParseTimeString(s string) (time.Time, TimeSpecialValue, error) {
	var tsv TimeSpecialValue
	var t time.Time
	var err error
	if s == "-infinity" {
		tsv = TSVNegativeInfinity
		return time.Time{}, tsv, nil
	}
	if s == "infinity" {
		tsv = TSVInfinity
		return time.Time{}, tsv, nil
	}
	switch l := len(s); {
	case l <= len(timeFormat):
		if s[2] == ':' {
			t, err = time.ParseInLocation(timeFormat, s, time.UTC)
			return t, tsv, err
		} else {
			t, err = time.ParseInLocation(dateFormat, s, time.UTC)
			return t, tsv, err
		}
	default:
		if s[10] == 'T' {
			t, err = time.Parse(time.RFC3339Nano, s)
			return t, tsv, err
		}
		if c := s[l-9]; c == '+' || c == '-' {
			t, err = time.Parse(timestamptzFormat, s)
			return t, tsv, err
		}
		if c := s[l-6]; c == '+' || c == '-' {
			t, err = time.Parse(timestamptzFormat2, s)
			return t, tsv, err
		}
		if c := s[l-3]; c == '+' || c == '-' {
			t, err = time.Parse(timestamptzFormat3, s)
			return t, tsv, err
		}
		t, err = time.ParseInLocation(timestampFormat, s, time.UTC)
	}
	return t, tsv, err
}

func AppendTime(b []byte, tm time.Time, flags int) []byte {
	if flags == 1 {
		b = append(b, '\'')
	}
	b = tm.UTC().AppendFormat(b, timestamptzFormat)
	if flags == 1 {
		b = append(b, '\'')
	}
	return b
}
