package main

import (
	"bufio"
	"fmt"
	"net/rpc"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

type Msg struct {
	User string
	Type string
	Text string
}

func main() {
	reader := bufio.NewReader(os.Stdin)

	fmt.Println("=========================================")
	fmt.Println("   Distributed RPC Chat Client           ")
	fmt.Println("=========================================")

	fmt.Print("Enter server address (default 127.0.0.1:9999): ")
	serverAddr, _ := reader.ReadString('\n')
	serverAddr = strings.TrimSpace(serverAddr)
	if serverAddr == "" {
		serverAddr = "127.0.0.1:9999"
	}

	client, err := rpc.Dial("tcp", serverAddr)
	if err != nil {
		fmt.Printf("Failed to connect to server at %s: %v\n", serverAddr, err)
		return
	}
	defer client.Close()
	fmt.Println("Connected to server successfully!")

	var username string
	for {
		fmt.Print("Enter your username: ")
		input, _ := reader.ReadString('\n')
		username = strings.TrimSpace(input)
		if username != "" {
			break
		}
		fmt.Println("Username cannot be empty.")
	}

	var joinReply bool
	err = client.Call("MyServer.Join", &username, &joinReply)
	if err != nil {
		fmt.Printf("Error joining chat server: %v\n", err)
		return
	}
	fmt.Printf("Welcome '%s'! Joined chat successfully.\n", username)
	fmt.Println("Type a message and press Enter to send. Type /exit to quit.")
	fmt.Println("-----------------------------------------")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	done := make(chan struct{})

	// Goroutine to periodically poll messages from server
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				var messages []Msg
				err := client.Call("MyServer.Get", &username, &messages)
				if err != nil {
					return
				}
				for _, msg := range messages {
					if msg.Type == "join" || msg.Type == "leave" {
						fmt.Printf("\n*** Notification: %s ***\n> ", msg.Text)
					} else {
						fmt.Printf("\n[%s]: %s\n> ", msg.User, msg.Text)
					}
				}
			}
		}
	}()

	cleanup := func() {
		select {
		case <-done:
			// already closed
		default:
			close(done)
			var leaveReply bool
			_ = client.Call("MyServer.Leave", &username, &leaveReply)
			fmt.Println("\nDisconnected from chat.")
		}
	}

	go func() {
		<-sigChan
		cleanup()
		os.Exit(0)
	}()

	for {
		fmt.Print("> ")
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		text := strings.TrimSpace(line)
		if text == "" {
			continue
		}

		if text == "/exit" || text == "/quit" {
			cleanup()
			break
		}

		msg := Msg{
			User: username,
			Type: "msg",
			Text: text,
		}
		var sendReply bool
		err = client.Call("MyServer.Send", &msg, &sendReply)
		if err != nil {
			fmt.Printf("Error sending message: %v\n", err)
			break
		}
	}
}
