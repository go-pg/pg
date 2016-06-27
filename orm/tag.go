package orm

import (
	"bytes"
	"strings"
)

type tagOptions string

func (o tagOptions) Get(name string) (string, bool) {
	s := string(o)
	for len(s) > 0 {
		var next string
		idx := strings.IndexByte(s, ',')
		if idx >= 0 {
			s, next = s[:idx], s[idx+1:]
		}
		if strings.HasPrefix(s, name) {
			return s[len(name):], true
		}
		s = next
	}
	return "", false
}

func parseTag(tagStr string) (string, tagOptions) {
	tag := []byte(tagStr)
	if idx := bytes.IndexByte(tag, ','); idx != -1 {
		return string(tag[:idx]), tagOptions(tag[idx+1:])
	}
	return tagStr, ""
}
