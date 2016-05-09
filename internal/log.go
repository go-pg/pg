package internal

import (
	"fmt"
	"log"
	"path/filepath"
	"runtime"
	"strings"
)

var (
	Logger      *log.Logger
	QueryLogger *log.Logger
)

func Logf(s string, args ...interface{}) {
	if Logger == nil {
		return
	}
	Logger.Output(2, fmt.Sprintf(s, args...))
}

func LogQuery(query string) {
	if QueryLogger == nil {
		return
	}
	file, line := fileLine(2)
	QueryLogger.Printf("%s:%d: %s", file, line, strings.TrimRight(query, "\t\n"))
}

const packageName = "gopkg.in/pg.v4"

func fileLine(depth int) (string, int) {
	for i := depth; ; i++ {
		_, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}
		if strings.Contains(file, packageName) {
			continue
		}
		return filepath.Base(file), line
	}
	return "", 0
}
