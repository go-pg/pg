package pg

func (db *DB) Pool() *connPool {
	return db.pool
}

func (ln *Listener) Conn() *conn {
	return ln._cn
}
