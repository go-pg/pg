package pg

import (
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/go-pg/pg/v10/internal/pool"
	"github.com/stretchr/testify/assert"
)

/*
	The test is for testing the case that sending a cancel request when the timeout from connection comes earlier than ctx.Done().
*/
func Test_baseDB_withConn(t *testing.T) {
	b := mockBaseDB{}
	b.init()
	b.pool = &mockPooler{}
	ctx, _ := context.WithDeadline(context.TODO(), time.Now().Add(1000*time.Second)) // Make a deadline in context further than the timeout of connection.
	b.withConn(ctx, func(context.Context, *pool.Conn) error {
		// Immediately returns the error, so it is faster than the ctx.Done() returns. The error code here according to the function `isBadConn`.
		return &mockPGError{map[byte]string{byte('C'): "57014"}}
	})
	// In the new change, a cancel request is sent to db and that connection is removed from the connection pool.
	// Check if the cancel request, its code int32(80877102), is sent.
	assert.Equal(t, int32(80877102), b.pool.(*mockPooler).mockConn.cancelCode)
	assert.True(t, b.pool.(*mockPooler).toRemove)
}

type mockBaseDB struct {
	baseDB
}

func (m *mockBaseDB) init() {
	m.opt = &Options{}
}

type mockPooler struct {
	conn     *pool.Conn
	toRemove bool
	mockConn mockConn
}

func (m *mockPooler) NewConn(ctx context.Context) (*pool.Conn, error) {
	m.conn = &pool.Conn{ProcessID: 123, SecretKey: 234, Inited: true}
	m.mockConn = mockConn{}
	m.conn.SetNetConn(&m.mockConn)
	return m.conn, nil
}

func (m *mockPooler) CloseConn(conn *pool.Conn) error {
	return nil
}

func (m *mockPooler) Get(ctx context.Context) (*pool.Conn, error) {
	return &pool.Conn{ProcessID: 123, SecretKey: 234, Inited: true}, nil
}

func (m *mockPooler) Put(ctx context.Context, conn *pool.Conn) {
	return
}

func (m *mockPooler) Remove(ctx context.Context, conn *pool.Conn, err error) {
	m.toRemove = true
	return
}

func (m *mockPooler) Len() int {
	return 1
}

func (m *mockPooler) IdleLen() int {
	return 1
}

func (m *mockPooler) Stats() *pool.Stats {
	return nil
}

func (m *mockPooler) Close() error {
	return nil
}

type mockPGError struct {
	M map[byte]string
}

func (m *mockPGError) Error() string {
	return ""
}

func (m *mockPGError) Field(field byte) string {
	return m.M[field]
}

func (m *mockPGError) IntegrityViolation() bool {
	return false
}

type mockConn struct {
	cancelCode int32
}

func (m *mockConn) Read(b []byte) (n int, err error) {
	return 0, nil
}

func (m *mockConn) Write(b []byte) (n int, err error) {
	m.cancelCode = int32(binary.BigEndian.Uint32(b[4:8]))
	return 0, nil
}

func (m *mockConn) Close() error {
	return nil
}

func (m *mockConn) LocalAddr() net.Addr {
	return nil
}

func (m *mockConn) RemoteAddr() net.Addr {
	return nil
}

func (m *mockConn) SetDeadline(t time.Time) error {
	return nil
}

func (m *mockConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (m *mockConn) SetWriteDeadline(t time.Time) error {
	return nil
}
