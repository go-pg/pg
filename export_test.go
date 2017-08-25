package pg

import "github.com/go-pg/pg/internal/pool"

func (db *DB) Pool() pool.Pooler {
	return db.pool
}

func (ln *Listener) CurrentConn() *pool.Conn {
	return ln.cn
}
