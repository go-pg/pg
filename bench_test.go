package pg_test

import (
	"database/sql"
	"math/rand"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"

	"gopkg.in/pg.v3"
)

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

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

type record struct {
	Num1, Num2, Num3 int64
	Str1, Str2, Str3 string
}

type records []*record

func (rs *records) New() interface{} {
	r := &record{}
	*rs = append(*rs, r)
	return r
}

func seedDB(db *pg.DB) error {
	_, err := db.Exec(`DROP TABLE IF EXISTS bench_test`)
	if err != nil {
		return err
	}

	_, err = db.Exec(`
		CREATE TABLE bench_test(
			num1 serial,
			num2 serial,
			num3 serial,
			str1 text,
			str2 text,
			str3 text
		)
	`)
	if err != nil {
		return err
	}

	for i := 0; i < 1000; i++ {
		_, err := db.Exec(`
			INSERT INTO bench_test (str1, str2, str3) VALUES (?, ?, ?)
		`, randSeq(100), randSeq(200), randSeq(300))
		if err != nil {
			return err
		}
	}

	return nil
}

func BenchmarkQuery(b *testing.B) {
	db := pgdb()
	defer db.Close()

	if err := seedDB(db); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var rs records
		_, err := db.Query(&rs, `SELECT * FROM bench_test`)
		if err != nil {
			b.Fatal(err)
		}
		if len(rs) != 1000 {
			b.Fatalf("got %d, wanted 1000", len(rs))
		}
	}
}

func BenchmarkQueryStdlibPq(b *testing.B) {
	db := pgdb()
	defer db.Close()

	if err := seedDB(db); err != nil {
		b.Fatal(err)
	}

	pqdb, err := pqdb()
	if err != nil {
		b.Fatal(err)
	}
	defer pqdb.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := pqdb.Query(`SELECT * FROM bench_test`)
		if err != nil {
			b.Fatal(err)
		}

		var rs []*record
		for rows.Next() {
			var rec record
			err := rows.Scan(&rec.Num1, &rec.Num2, &rec.Num3, &rec.Str1, &rec.Str2, &rec.Str3)
			if err != nil {
				b.Fatal(err)
			}
			rs = append(rs, &rec)
		}
		rows.Close()

		if len(rs) != 1000 {
			b.Fatalf("got %d, wanted 1000", len(rs))
		}
	}
}

func BenchmarkQueryRow(b *testing.B) {
	db := pgdb()
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var dst numLoader
		_, err := db.QueryOne(&dst, `SELECT ?::bigint AS num`, 1)
		if err != nil {
			b.Fatal(err)
		}
		if dst.Num != 1 {
			b.Fatalf("got %d, wanted 1", dst.Num)
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
		var dst numLoader
		_, err := stmt.QueryOne(&dst, 1)
		if err != nil {
			b.Fatal(err)
		}
		if dst.Num != 1 {
			b.Fatalf("got %d, wanted 1", dst.Num)
		}
	}
}

func BenchmarkQueryRowLoadInto(b *testing.B) {
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
			b.Fatalf("got %d, wanted 1", n)
		}
	}
}

func BenchmarkQueryRowStmtLoadInto(b *testing.B) {
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
			b.Fatalf("got %d, wanted 1", n)
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
			b.Fatalf("got %d, wanted 1", n)
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
