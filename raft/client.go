package raft

import (
	"encoding/gob"
	"errors"
	"net"
	"sync"
)

type RaftClient struct {
	leaderAddr *net.TCPAddr

	mu sync.RWMutex
}

func NewRaftClient(server *RaftStore) (*RaftClient, error) {
	if server == nil {
		return nil, errors.New("server must not be nil")
	}

	return &RaftClient{
		leaderAddr: server.rpcServer.Addr().(*net.TCPAddr),
	}, nil
}

func (r *RaftClient) clientOp(cmd Command) (*Response, error) {
	r.mu.RLock()
	client, err := net.DialTCP("tcp", nil, r.leaderAddr)
	r.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	defer client.Close()

	encoder := gob.NewEncoder(client)
	if err := encoder.Encode(cmd); err != nil {
		return nil, err
	}

	var clientResponse Response
	decoder := gob.NewDecoder(client)
	if err := decoder.Decode(&clientResponse); err != nil {
		return nil, err
	}
	r.mu.RLock()
	if clientResponse.Leader != "" && clientResponse.Leader != r.leaderAddr.String() {
		addr, err := net.ResolveTCPAddr("tcp", clientResponse.Leader)
		r.mu.RUnlock()
		if err != nil {
			return nil, err
		}
		r.mu.Lock()
		r.leaderAddr = addr
		r.mu.Unlock()
	} else {
		r.mu.RUnlock()
	}
	return &clientResponse, nil
}

func (r *RaftClient) Put(key string, value []byte) error {
	clientResponse, err := r.clientOp(Command{
		Op:    "put",
		Key:   key,
		Value: value,
	})
	if err != nil {
		return err
	}
	return clientResponse.Err
}

func (r *RaftClient) Get(key string) ([]byte, error) {
	clientResponse, err := r.clientOp(Command{
		Op:  "get",
		Key: key,
	})
	if err != nil {
		return nil, err
	}
	return clientResponse.Value, clientResponse.Err
}

func (r *RaftClient) Keys() ([]string, error) {
	clientResponse, err := r.clientOp(Command{
		Op: "keys",
	})
	if err != nil {
		return nil, err
	}
	return clientResponse.Keys, clientResponse.Err
}

func (r *RaftClient) Delete(key string) error {
	clientResponse, err := r.clientOp(Command{
		Op:  "delete",
		Key: key,
	})
	if err != nil {
		return err
	}
	return clientResponse.Err
}

func (r *RaftClient) Close() {}
