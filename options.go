package pg

import (
	"net"
	"time"

	"gopkg.in/pg.v4/internal/pool"
)

// Database connection options.
type Options struct {
	// The network type, either tcp or unix.
	// Default is tcp.
	Network string
	// TCP host:port or Unix socket depending on Network.
	Addr     string
	User     string
	Password string
	Database string
	// Whether to use secure TCP/IP connections (TLS).
	SSL bool

	// PostgreSQL run-time configuration parameters to be set on connection.
	Params map[string]interface{}

	// The maximum number of retries before giving up.
	// Default is to not retry failed queries.
	MaxRetries int

	// The deadline for establishing new connections. If reached,
	// dial will fail with a timeout.
	// Default is 5 seconds.
	DialTimeout time.Duration
	// The timeout for socket reads. If reached, commands will fail
	// with a timeout error instead of blocking.
	// Default is no timeout.
	ReadTimeout time.Duration
	// The timeout for socket writes. If reached, commands will fail
	// with a timeout error instead of blocking.
	// Default is no timeout.
	WriteTimeout time.Duration

	// The maximum number of open socket connections.
	// Default is 20 connections.
	PoolSize int
	// The amount of time client waits for free connection if all
	// connections are busy before returning an error.
	// Default is 5 seconds.
	PoolTimeout time.Duration
	// The amount of time after which client closes idle connections.
	// Default is to not close idle connections.
	IdleTimeout time.Duration
	// The frequency of idle checks.
	// Default is 1 minute.
	IdleCheckFrequency time.Duration
}

func (opt *Options) getNetwork() string {
	if opt == nil || opt.Network == "" {
		return "tcp"
	}
	return opt.Network
}

func (opt *Options) getAddr() string {
	if opt.Addr != "" {
		return opt.Addr
	}
	if opt.getNetwork() == "unix" {
		return "/var/run/postgresql/.s.PGSQL.5432"
	}
	return "localhost:5432"
}

func (opt *Options) getUser() string {
	if opt == nil || opt.User == "" {
		return ""
	}
	return opt.User
}

func (opt *Options) getPassword() string {
	if opt == nil || opt.Password == "" {
		return ""
	}
	return opt.Password
}

func (opt *Options) getDatabase() string {
	if opt == nil || opt.Database == "" {
		return ""
	}
	return opt.Database
}

func (opt *Options) getPoolSize() int {
	if opt == nil || opt.PoolSize == 0 {
		return 20
	}
	return opt.PoolSize
}

func (opt *Options) getPoolTimeout() time.Duration {
	if opt == nil || opt.PoolTimeout == 0 {
		return 5 * time.Second
	}
	return opt.PoolTimeout
}

func (opt *Options) getDialTimeout() time.Duration {
	if opt.DialTimeout == 0 {
		return 5 * time.Second
	}
	return opt.DialTimeout
}

func (opt *Options) getIdleTimeout() time.Duration {
	return opt.IdleTimeout
}

func (opt *Options) getIdleCheckFrequency() time.Duration {
	if opt.IdleCheckFrequency == 0 {
		return time.Minute
	}
	return opt.IdleCheckFrequency
}

func (opt *Options) getSSL() bool {
	return opt.SSL
}

func (opt *Options) getDialer() func() (net.Conn, error) {
	return func() (net.Conn, error) {
		return net.DialTimeout(opt.getNetwork(), opt.getAddr(), opt.getDialTimeout())
	}
}

func newConnPool(opt *Options) *pool.ConnPool {
	p := pool.NewConnPool(
		opt.getDialer(),
		opt.getPoolSize(),
		opt.getPoolTimeout(),
		opt.getIdleTimeout(),
		opt.getIdleCheckFrequency(),
	)
	p.OnClose = func(cn *pool.Conn) error {
		return terminateConn(cn)
	}
	return p
}
