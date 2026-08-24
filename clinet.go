package main

import (
	"bufio"
	"fmt"
	"net/rpc"
	"os"
	"strings"
	"time"
)

type Msg struct {
	User string
	Type string
	Text string
}

func main() {
	name := os.Args[1]

	c, _ := rpc.Dial("tcp", "localhost:9999")

	var r bool
	c.Call("MyServer.Join", &name, &r)

	go func() {
		for {
			var m []Msg
			c.Call("MyServer.Get", &name, &m)

			for _, x := range m {
				fmt.Println(x.Text)
			}

			time.Sleep(time.Second)
		}
	}()

	reader := bufio.NewReader(os.Stdin)
	for {
		in, _ := reader.ReadString('\n')
		in = strings.TrimSpace(in)

		if in == "/exit" {
			c.Call("MyServer.Leave", &name, &r)
			os.Exit(0)
		}

		if in != "" {
			m := Msg{name, "msg", name + ": " + in}
			c.Call("MyServer.Send", &m, &r)
		}
	}
}
