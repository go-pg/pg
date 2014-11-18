package pg_test

import (
	"database/sql"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"

	"gopkg.in/pg.v3"
)

func pgdb() *pg.DB {
	return pg.Connect(&pg.Options{
		User:     "postgres",
		Database: "test",
	})
}

func pqdb() (*sql.DB, error) {
	return sql.Open("postgres", "user=postgres dbname=test")
}

func mysqldb() (*sql.DB, error) {
	return sql.Open("mysql", "root:root@tcp(localhost:3306)/test")
}

func BenchmarkQueryRow(b *testing.B) {
	db := pgdb()
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var n int64
		_, err := db.QueryOne(pg.LoadInto(&n), `SELECT ? AS num`, 1)
		if err != nil {
			b.Fatal(err)
		}
		if n != 1 {
			b.Fatalf("n != 1")
		}
	}
}

func BenchmarkQueryRowStmt(b *testing.B) {
	db := pgdb()
	defer db.Close()

	stmt, err := db.Prepare(`SELECT $1::bigint AS num`)
	if err != nil {
		b.Fatal(err)
	}
	defer stmt.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var n int64
		_, err := stmt.QueryOne(pg.LoadInto(&n), 1)
		if err != nil {
			b.Fatal(err)
		}
		if n != 1 {
			b.Fatalf("n != 1")
		}
	}
}

func BenchmarkQueryRowStdlibPq(b *testing.B) {
	db, err := pqdb()
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var n int64
		r := db.QueryRow(`SELECT $1::bigint AS num`, 1)
		if err := r.Scan(&n); err != nil {
			b.Fatal(err)
		}
		if n != 1 {
			b.Fatalf("n != 1")
		}
	}
}

func BenchmarkQueryRowWithoutParamsStdlibPq(b *testing.B) {
	db, err := pqdb()
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var n int64
		r := db.QueryRow("SELECT 1::bigint AS num")
		if err := r.Scan(&n); err != nil {
			b.Fatal(err)
		}
		if n != 1 {
			b.Fatalf("got %d, wanted 1", n)
		}
	}
}

func BenchmarkQueryRowStdlibMySQL(b *testing.B) {
	db, err := mysqldb()
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var n int64
		r := db.QueryRow("SELECT ? AS num", 1)
		if err := r.Scan(&n); err != nil {
			b.Fatal(err)
		}
		if n != 1 {
			b.Fatalf("got %d, wanted 1", n)
		}
	}
}

func BenchmarkQueryRowStmtStdlibPq(b *testing.B) {
	db, err := pqdb()
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	stmt, err := db.Prepare(`SELECT $1::bigint AS num`)
	if err != nil {
		b.Fatal(err)
	}
	defer stmt.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var n int64
		r := stmt.QueryRow(1)
		if err := r.Scan(&n); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkExec(b *testing.B) {
	db := pgdb()
	defer db.Close()

	_, err := db.Exec(
		`CREATE TEMP TABLE exec_test(id bigint, name varchar(500))`)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := db.Exec(`INSERT INTO exec_test(id, name) VALUES(?, ?)`, 1, "hello world")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkExecWithError(b *testing.B) {
	db := pgdb()
	defer db.Close()

	_, err := db.Exec(
		`CREATE TEMP TABLE exec_with_error_test(id bigint PRIMARY KEY, name varchar(500))`)
	if err != nil {
		b.Fatal(err)
	}

	_, err = db.Exec(`
		INSERT INTO exec_with_error_test(id, name) VALUES(?, ?)
	`, 1, "hello world")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := db.Exec(`INSERT INTO exec_with_error_test(id) VALUES(?)`, 1)
		if err == nil {
			b.Fatalf("got nil error, expected IntegrityError")
		} else if _, ok := err.(*pg.IntegrityError); !ok {
			b.Fatalf("got " + err.Error() + ", expected IntegrityError")
		}
	}
}

func BenchmarkExecStmt(b *testing.B) {
	db := pgdb()
	defer db.Close()

	_, err := db.Exec(`CREATE TEMP TABLE statement_exec(id bigint, name varchar(500))`)
	if err != nil {
		b.Fatal(err)
	}

	stmt, err := db.Prepare(`INSERT INTO statement_exec(id, name) VALUES($1, $2)`)
	if err != nil {
		b.Fatal(err)
	}
	defer stmt.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := stmt.Exec(1, "hello world")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkExecStmtStdlibPq(b *testing.B) {
	db, err := pqdb()
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`CREATE TEMP TABLE statement_exec(id bigint, name varchar(500))`)
	if err != nil {
		b.Fatal(err)
	}

	stmt, err := db.Prepare(`INSERT INTO statement_exec(id, name) VALUES($1, $2)`)
	if err != nil {
		b.Fatal(err)
	}
	defer stmt.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := stmt.Exec(1, "hello world")
		if err != nil {
			b.Fatal(err)
		}
	}
}
