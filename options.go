package pg

import (
	"crypto/tls"
	"net"
	"time"

	"gopkg.in/pg.v4/internal/pool"
)

// Database connection options.
type Options struct {
	// Network type, either tcp or unix.
	// Default is tcp.
	Network string
	// TCP host:port or Unix socket depending on Network.
	Addr     string
	User     string
	Password string
	Database string

	// Whether to use secure TCP/IP connections (TLS).
	// TODO: deprecated in favor of TLSConfig
	SSL bool
	// TLS config for secure connections.
	TLSConfig *tls.Config

	// PostgreSQL run-time configuration parameters to be set on connection.
	Params map[string]interface{}

	// Maximum number of retries before giving up.
	// Default is to not retry failed queries.
	MaxRetries int

	// Dial timeout for establishing new connections.
	// Default is 5 seconds.
	DialTimeout time.Duration
	// Timeout for socket reads. If reached, commands will fail
	// with a timeout instead of blocking.
	ReadTimeout time.Duration
	// Timeout for socket writes. If reached, commands will fail
	// with a timeout instead of blocking.
	WriteTimeout time.Duration

	// Maximum number of socket connections.
	// Default is 20 connections.
	PoolSize int
	// Amount of time client waits for free connection if all
	// connections are busy before returning an error.
	// Default is 5 seconds.
	PoolTimeout time.Duration
	// Amount of time after which client closes idle connections.
	// Default is to not close idle connections.
	IdleTimeout time.Duration
	// Frequency of idle checks.
	// Default is 1 minute.
	IdleCheckFrequency time.Duration
}

func (opt *Options) init() {
	if opt.Network == "" {
		opt.Network = "tcp"
	}

	if opt.Addr == "" {
		switch opt.Network {
		case "tcp":
			opt.Addr = "localhost:5432"
		case "unix":
			opt.Addr = "/var/run/postgresql/.s.PGSQL.5432"
		}
	}

	if opt.PoolSize == 0 {
		opt.PoolSize = 20
	}

	if opt.PoolTimeout == 0 {
		opt.PoolTimeout = 5 * time.Second
	}

	if opt.DialTimeout == 0 {
		opt.DialTimeout = 5 * time.Second
	}

	if opt.IdleCheckFrequency == 0 {
		opt.IdleCheckFrequency = time.Minute
	}
}

func (opt *Options) getDialer() func() (net.Conn, error) {
	return func() (net.Conn, error) {
		return net.DialTimeout(opt.Network, opt.Addr, opt.DialTimeout)
	}
}

func newConnPool(opt *Options) *pool.ConnPool {
	p := pool.NewConnPool(
		opt.getDialer(),
		opt.PoolSize,
		opt.PoolTimeout,
		opt.IdleTimeout,
		opt.IdleCheckFrequency,
	)
	p.OnClose = func(cn *pool.Conn) error {
		return terminateConn(cn)
	}
	return p
}
