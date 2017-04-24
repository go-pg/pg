package pg

import "github.com/go-pg/pg/internal/pool"

func (db *DB) Pool() *pool.ConnPool {
	return db.pool
}

func (ln *Listener) CurrentConn() *pool.Conn {
	return ln.cn
}
