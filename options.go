package pg

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/go-pg/pg/internal/pool"
)

// Database connection options.
type Options struct {
	// Network type, either tcp or unix.
	// Default is tcp.
	Network string
	// TCP host:port or Unix socket depending on Network.
	Addr string

	// Dialer creates new network connection and has priority over
	// Network and Addr options.
	Dialer func(network, addr string) (net.Conn, error)

	User     string
	Password string
	Database string

	// TLS config for secure connections.
	TLSConfig *tls.Config

	// Maximum number of retries before giving up.
	// Default is to not retry failed queries.
	MaxRetries int
	// Whether to retry queries cancelled because of statement_timeout.
	RetryStatementTimeout bool

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
	// Time for which client waits for free connection if all
	// connections are busy before returning an error.
	// Default is 5 seconds.
	PoolTimeout time.Duration
	// Time after which client closes idle connections.
	// Default is to not close idle connections.
	IdleTimeout time.Duration
	// Connection age at which client retires (closes) the connection.
	// Primarily useful with proxies like HAProxy.
	// Default is to not close aged connections.
	MaxAge time.Duration
	// Frequency of idle checks.
	// Default is 1 minute.
	IdleCheckFrequency time.Duration

	// When true Tx does not issue BEGIN, COMMIT, or ROLLBACK.
	// Also underlying database connection is immediately returned to the pool.
	// This is primarily useful for running your database tests in one big
	// transaction, because PostgreSQL does not support nested transactions.
	DisableTransaction bool
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
		if opt.ReadTimeout != 0 {
			opt.PoolTimeout = opt.ReadTimeout + time.Second
		} else {
			opt.PoolTimeout = 30 * time.Second
		}
	}

	if opt.DialTimeout == 0 {
		opt.DialTimeout = 5 * time.Second
	}

	if opt.IdleCheckFrequency == 0 {
		opt.IdleCheckFrequency = time.Minute
	}
}

// ParseURL parses an URL into options that can be used to connect to PostgreSQL.
func ParseURL(sURL string) (*Options, error) {
	parsedUrl, err := url.Parse(sURL)
	if err != nil {
		return nil, err
	}

	// scheme
	if parsedUrl.Scheme != "postgres" {
		return nil, errors.New("pg: invalid scheme: " + parsedUrl.Scheme)
	}

	// host and port
	options := &Options{
		Addr: parsedUrl.Host,
	}
	if !strings.Contains(options.Addr, ":") {
		options.Addr = options.Addr + ":5432"
	}

	// username and password
	if parsedUrl.User != nil {
		options.User = parsedUrl.User.Username()

		if password, ok := parsedUrl.User.Password(); ok {
			options.Password = password
		}
	}

	if options.User == "" {
		options.User = "postgres"
	}

	// database
	if len(strings.Trim(parsedUrl.Path, "/")) > 0 {
		options.Database = parsedUrl.Path[1:]
	} else {
		return nil, errors.New("pg: database name not provided")
	}

	// ssl mode
	query, err := url.ParseQuery(parsedUrl.RawQuery)
	if err != nil {
		return nil, err
	}

	if sslMode, ok := query["sslmode"]; ok && len(sslMode) > 0 {
		switch sslMode[0] {
		case "allow":
			fallthrough
		case "prefer":
			options.TLSConfig = &tls.Config{InsecureSkipVerify: true}
		case "disable":
			options.TLSConfig = nil
		default:
			return nil, errors.New(fmt.Sprintf("pg: sslmode '%v' is not supported", sslMode[0]))
		}
	} else {
		options.TLSConfig = &tls.Config{InsecureSkipVerify: true}
	}

	delete(query, "sslmode")
	if len(query) > 0 {
		return nil, errors.New("pg: options other than 'sslmode' are not supported")
	}

	return options, nil
}

func (opt *Options) getDialer() func() (net.Conn, error) {
	if opt.Dialer != nil {
		return func() (net.Conn, error) {
			return opt.Dialer(opt.Network, opt.Addr)
		}
	}
	return func() (net.Conn, error) {
		return net.DialTimeout(opt.Network, opt.Addr, opt.DialTimeout)
	}
}

func newConnPool(opt *Options) *pool.ConnPool {
	return pool.NewConnPool(&pool.Options{
		Dial:               opt.getDialer(),
		PoolSize:           opt.PoolSize,
		PoolTimeout:        opt.PoolTimeout,
		IdleTimeout:        opt.IdleTimeout,
		IdleCheckFrequency: opt.IdleCheckFrequency,
		OnClose: func(cn *pool.Conn) error {
			return terminateConn(cn)
		},
	})
}
