package main

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/url"
	"sync"
)

// A protocol-aware reply-loss fault: it drops an actual backend COMMIT
// CommandComplete before the client can receive it. It does not fake a DB result.
type commitProxy struct {
	DSN      string
	Dropped  chan struct{}
	listener net.Listener
	sockets  []net.Conn
	mu       sync.Mutex
}

func newCommitProxy(ctx context.Context, dsn string) (*commitProxy, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &commitProxy{Dropped: make(chan struct{}), listener: listener}
	target := u.Host
	u.Host = listener.Addr().String()
	p.DSN = u.String()
	go func() {
		client, e := listener.Accept()
		if e != nil {
			return
		}
		server, e := net.Dial("tcp", target)
		if e != nil {
			client.Close()
			return
		}
		p.mu.Lock()
		p.sockets = append(p.sockets, client, server)
		p.mu.Unlock()
		defer client.Close()
		defer server.Close()
		go func() { _, _ = io.Copy(server, client) }()
		for {
			header := make([]byte, 5)
			if _, e = io.ReadFull(server, header); e != nil {
				return
			}
			n := int(binary.BigEndian.Uint32(header[1:]))
			if n < 4 || n > 1<<20 {
				return
			}
			body := make([]byte, n-4)
			if _, e = io.ReadFull(server, body); e != nil {
				return
			}
			if header[0] == 'C' && string(body) == "COMMIT\x00" {
				close(p.Dropped)
				return
			}
			if _, e = client.Write(header); e != nil {
				return
			}
			if _, e = client.Write(body); e != nil {
				return
			}
		}
	}()
	go func() { <-ctx.Done(); p.Close() }()
	return p, nil
}
func (p *commitProxy) Close() error {
	p.listener.Close()
	p.mu.Lock()
	defer p.mu.Unlock()
	var err error
	for _, c := range p.sockets {
		if e := c.Close(); e != nil && !errors.Is(e, net.ErrClosed) {
			err = e
		}
	}
	return err
}
