package pg

func (db *DB) Pool() *connPool {
	return db.pool
}

func (ln *Listener) CurrentConn() *conn {
	return ln._cn
}
