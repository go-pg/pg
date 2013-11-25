package pg

func (db *DB) Pool() *connPool {
	return db.pool
}
