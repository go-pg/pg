package internal

import (
	"io/ioutil"
	"log"
)

var Debug bool

var Logger = log.New(ioutil.Discard, "pg: ", log.LstdFlags)

func Debugf(s string, args ...interface{}) {
	if !Debug {
		return
	}
	Logger.Printf(s, args...)
}

func Logf(s string, args ...interface{}) {
	Logger.Printf(s, args...)
}
