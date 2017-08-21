package orm

import (
	"github.com/jinzhu/inflection"
)

var tableNameInflector func(string) string

func init() {
	SetTableNameInflector(inflection.Plural)
}

// SetTableNameInflector overrides the default func that pluralizes
// model name to get table name, e.g. my_article becomes my_articles.
func SetTableNameInflector(fn func(string) string) {
	tableNameInflector = fn
}
