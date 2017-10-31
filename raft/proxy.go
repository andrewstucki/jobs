package raft

import (
	"io"
	"net"
	"sync"
)

type proxy struct {
	once   sync.Once
	errors chan error
}

func (p *proxy) err(err error) {
	p.once.Do(func() {
		p.errors <- err
	})
}

func (p *proxy) pipe(src, dst io.ReadWriter) {
	buff := make([]byte, 0xffff)
	for {
		n, err := src.Read(buff)
		if err != nil {
			p.err(err)
			return
		}

		if _, err := dst.Write(buff[:n]); err != nil {
			p.err(err)
			return
		}
	}
}

func Proxy(local io.ReadWriteCloser, addr *net.TCPAddr) error {
	defer local.Close()
	remote, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		return err
	}
	defer remote.Close()

	p := &proxy{
		errors: make(chan error),
	}
	go p.pipe(local, remote)
	go p.pipe(remote, local)

	if err := <-p.errors; err != io.EOF {
		return err
	}
	return nil
}
