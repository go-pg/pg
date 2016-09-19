package pg

import "gopkg.in/pg.v5/internal/pool"

func (db *DB) Pool() *pool.ConnPool {
	return db.pool
}

func (ln *Listener) CurrentConn() *pool.Conn {
	return ln._cn
}
