package internal

import (
	"fmt"
	"log"
)

var Logger *log.Logger

func Logf(s string, args ...interface{}) {
	if Logger == nil {
		return
	}
	_ = Logger.Output(2, fmt.Sprintf(s, args...))
}
