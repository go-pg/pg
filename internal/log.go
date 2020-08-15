package internal

import (
	"log"
	"os"
)

var Logger = log.New(os.Stderr, "pg: ", log.LstdFlags|log.Lshortfile)

var Warn = log.New(os.Stderr, "WARN: pg: ", log.LstdFlags)

var Deprecated = log.New(os.Stderr, "DEPRECATED: pg: ", log.LstdFlags)
