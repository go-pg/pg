package internal

import (
	"log"
	"os"
)

var Logger = log.New(os.Stderr, "pg: ", log.LstdFlags|log.Lshortfile)
