package types

import "time"

const (
	dateFormat         = "2006-01-02"
	timeFormat         = "15:04:05.999999999"
	timestampFormat    = "2006-01-02 15:04:05.999999999"
	timestamptzFormat  = "2006-01-02 15:04:05.999999999-07:00:00"
	timestamptzFormat2 = "2006-01-02 15:04:05.999999999-07:00"
	timestamptzFormat3 = "2006-01-02 15:04:05.999999999-07"
)

func ParseTime(b []byte) (time.Time, error) {
	switch l := len(b); {
	case l <= len(dateFormat):
		return time.Parse(dateFormat, string(b))
	case l <= len(timeFormat):
		return time.Parse(timeFormat, string(b))
	default:
		if c := b[len(b)-9]; c == '+' || c == '-' {
			return time.Parse(timestamptzFormat, string(b))
		}
		if c := b[len(b)-6]; c == '+' || c == '-' {
			return time.Parse(timestamptzFormat2, string(b))
		}
		if c := b[len(b)-3]; c == '+' || c == '-' {
			return time.Parse(timestamptzFormat3, string(b))
		}
		return time.ParseInLocation(timestampFormat, string(b), time.Local)
	}
}

func AppendTime(b []byte, tm time.Time, quote int) []byte {
	if quote == 1 {
		b = append(b, '\'')
	}
	b = tm.AppendFormat(b, timestamptzFormat)
	if quote == 1 {
		b = append(b, '\'')
	}
	return b
}
