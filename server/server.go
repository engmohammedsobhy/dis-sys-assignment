package main

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"
)

type Msg struct {
	User string
	Type string
	Text string
}

type MyServer struct{}

var clientsMap = make(map[string][]Msg)
var m sync.Mutex

func (s *MyServer) Join(name *string, reply *bool) error {
	m.Lock()
	clientsMap[*name] = []Msg{}
	fmt.Println(*name + " joined")

	for k := range clientsMap {
		if k != *name {
			clientsMap[k] = append(clientsMap[k], Msg{*name, "join", *name + " joined chat"})
		}
	}
	m.Unlock()
	*reply = true
	return nil
}

func (s *MyServer) Leave(name *string, reply *bool) error {
	m.Lock()
	delete(clientsMap, *name)
	fmt.Println(*name + " left")

	for k := range clientsMap {
		if k != *name {
			clientsMap[k] = append(clientsMap[k], Msg{*name, "leave", *name + " left chat"})
		}
	}
	m.Unlock()
	*reply = true
	return nil
}

func (s *MyServer) Send(msg *Msg, reply *bool) error {
	m.Lock()
	for k := range clientsMap {
		if k != msg.User {
			clientsMap[k] = append(clientsMap[k], *msg)
		}
	}
	m.Unlock()
	*reply = true
	return nil
}

func (s *MyServer) Get(name *string, reply *[]Msg) error {
	m.Lock()
	*reply = clientsMap[*name]
	clientsMap[*name] = []Msg{}
	m.Unlock()
	return nil
}

func main() {
	srv := new(MyServer)
	rpc.Register(srv)

	l, err := net.Listen("tcp", ":9999")
	if err != nil {
		fmt.Printf("Error starting server on port 9999: %v\n", err)
		return
	}
	defer l.Close()
	fmt.Println("Server started on :9999")

	rpc.Accept(l)
}
