package raft

import (
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/hashicorp/raft"
)

type Response struct {
	Value  []byte
	Keys   []string
	Err    error
	Leader string
}

var TCPResolutionError = Response{Value: nil, Err: errors.New("error resolving leader address")}

func respond(w io.Writer, r Response) error {
	encoder := gob.NewEncoder(w)
	return encoder.Encode(r)
}

func (s *RaftStore) Listen(hostPortPair string, initialized chan bool) error {
	server, err := net.Listen("tcp", hostPortPair)
	if err != nil {
		initialized <- true
		return err
	}
	s.rpcServer = server

	initialized <- true

	for {
		connection, err := server.Accept()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		go s.handleConnection(connection)
	}
}

func (s *RaftStore) handleConnection(conn net.Conn) {
	if s.raft.State() != raft.Leader {
		leaderAddress := s.raft.Leader()
		addr, err := net.ResolveTCPAddr("tcp", string(leaderAddress))
		if err != nil {
			respond(conn, TCPResolutionError)
			conn.Close()
			return
		}
		remoteAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", addr.IP.String(), addr.Port+1))
		if err != nil {
			respond(conn, TCPResolutionError)
			conn.Close()
			return
		}
		Proxy(conn, remoteAddr)
		return
	}

	defer conn.Close()

	var c Command
	decoder := gob.NewDecoder(conn)
	if err := decoder.Decode(&c); err != nil {
		respond(conn, Response{Value: nil, Err: errors.New("error unmarshalling command"), Leader: s.rpcServer.Addr().String()})
		return
	}

	switch c.Op {
	case "put":
		respond(conn, Response{
			Err:    s.put(c.Key, c.Value),
			Leader: s.rpcServer.Addr().String(),
		})
		return
	case "delete":
		respond(conn, Response{
			Err:    s.delete(c.Key),
			Leader: s.rpcServer.Addr().String(),
		})
		return
	case "get":
		value, err := s.get(c.Key)
		respond(conn, Response{
			Value:  value,
			Err:    err,
			Leader: s.rpcServer.Addr().String(),
		})
		return
	case "keys":
		keys, err := s.keys()
		respond(conn, Response{
			Keys:   keys,
			Err:    err,
			Leader: s.rpcServer.Addr().String(),
		})
		return
	default:
		respond(conn, Response{Value: nil, Err: errors.New("unknown command"), Leader: s.rpcServer.Addr().String()})
		return
	}
}
