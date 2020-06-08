package internal

import (
	"log"
	"os"
)

var Logger = log.New(os.Stderr, "pg: ", log.LstdFlags|log.Lshortfile)

var Deprecated = log.New(os.Stderr, "DEPRECATED: pg: ", log.LstdFlags)
