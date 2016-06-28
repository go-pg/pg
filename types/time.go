package types

import (
	"time"

	"gopkg.in/pg.v4/internal"
)

const (
	dateFormat         = "2006-01-02"
	timeFormat         = "15:04:05.999999999"
	timestampFormat    = "2006-01-02 15:04:05.999999999"
	timestamptzFormat  = "2006-01-02 15:04:05.999999999-07:00"
	timestamptzFormat2 = "2006-01-02 15:04:05.999999999-07"
	timestamptzFormat3 = "2006-01-02 15:04:05.999999999-07:00:00"
)

func ParseTime(b []byte) (time.Time, error) {
	s := internal.BytesToString(b)
	switch l := len(b); {
	case l <= len(dateFormat):
		return time.Parse(dateFormat, s)
	case l <= len(timeFormat):
		return time.Parse(timeFormat, s)
	default:
		if c := b[len(b)-6]; c == '+' || c == '-' {
			return time.Parse(timestamptzFormat, s)
		}
		if c := b[len(b)-3]; c == '+' || c == '-' {
			return time.Parse(timestamptzFormat2, s)
		}
		if c := b[len(b)-9]; c == '+' || c == '-' {
			return time.Parse(timestamptzFormat3, s)
		}
		return time.ParseInLocation(timestampFormat, s, time.Local)
	}
}

func AppendTime(b []byte, tm time.Time, quote int) []byte {
	if quote == 1 {
		b = append(b, '\'')
	}
	b = append(b, tm.Local().Format(timestamptzFormat)...)
	if quote == 1 {
		b = append(b, '\'')
	}
	return b
}
