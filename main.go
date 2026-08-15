package main

import (
	"bufio"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
)

type Message struct {
	Sender string
	Text   string
	Type   string
}

type Client struct {
	Name        string
	DisplayPipe chan Message
}

type ChatServer struct {
	JoinPipe    chan *Client
	MessagePipe chan Message
	LeavePipe   chan string
	shutdown    chan struct{}

	clients map[string]*Client
	mu      sync.Mutex
	wg      sync.WaitGroup
}

func NewServer() *ChatServer {
	return &ChatServer{
		JoinPipe:    make(chan *Client),
		MessagePipe: make(chan Message),
		LeavePipe:   make(chan string),
		shutdown:    make(chan struct{}),
		clients:     make(map[string]*Client),
	}
}

func (s *ChatServer) Run() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			select {
			case client := <-s.JoinPipe:
				s.mu.Lock()
				s.clients[client.Name] = client
				s.mu.Unlock()

				s.broadcast(Message{
					Sender: client.Name,
					Type:   "join",
					Text:   fmt.Sprintf("User %s joined the chat.", client.Name),
				})

			case name := <-s.LeavePipe:
				s.mu.Lock()
				if client, exists := s.clients[name]; exists {
					close(client.DisplayPipe)
					delete(s.clients, name)

					s.broadcast(Message{
						Sender: name,
						Type:   "leave",
						Text:   fmt.Sprintf("User %s left the chat.", name),
					})
				}
				s.mu.Unlock()

			case msg := <-s.MessagePipe:
				s.broadcast(msg)

			case <-s.shutdown:
				s.mu.Lock()
				for name, client := range s.clients {
					close(client.DisplayPipe)
					delete(s.clients, name)
				}
				s.mu.Unlock()
				return
			}
		}
	}()
}

func (s *ChatServer) broadcast(msg Message) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, client := range s.clients {
		if client.Name != msg.Sender {
			client.DisplayPipe <- msg
		}
	}
}
func (s *ChatServer) Stop() {
	close(s.shutdown)
	s.wg.Wait()
	fmt.Println("\nServer shut down cleanly. All goroutines stopped.")
}

func (s *ChatServer) runClient(c *Client) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for msg := range c.DisplayPipe {
			fmt.Printf("\n[Notification for %s] %s\n> ", c.Name, msg.Text)
		}
	}()
}

func main() {
	server := NewServer()
	server.Run()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		server.Stop()
		os.Exit(0)
	}()

	reader := bufio.NewReader(os.Stdin)
	var activeUser string

	printMenu()

	for {
		fmt.Print("> ")
		input, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		input = strings.TrimSpace(input)
		if input == "" {
			continue
		}

		parts := strings.SplitN(input, " ", 2)
		command := strings.ToLower(parts[0])
		arg := ""
		if len(parts) > 1 {
			arg = parts[1]
		}

		switch command {
		case "/create":
			if arg == "" {
				fmt.Println("Usage: /create <username>")
				continue
			}
			server.mu.Lock()
			_, exists := server.clients[arg]
			server.mu.Unlock()

			if exists {
				fmt.Println("Error: Username already taken.")
			} else {
				client := &Client{
					Name:        arg,
					DisplayPipe: make(chan Message, 100),
				}
				server.runClient(client)
				server.JoinPipe <- client
				fmt.Printf("User '%s' created.\n", arg)
				if activeUser == "" {
					activeUser = arg
					fmt.Printf("Auto-selected active user: %s\n", activeUser)
				}
			}

		case "/list":
			server.mu.Lock()
			fmt.Println("--- Connected Users ---")
			if len(server.clients) == 0 {
				fmt.Println("(none)")
			} else {
				for name := range server.clients {
					if name == activeUser {
						fmt.Printf("- %s (active)\n", name)
					} else {
						fmt.Printf("- %s\n", name)
					}
				}
			}
			server.mu.Unlock()

		case "/select":
			if arg == "" {
				fmt.Println("Usage: /select <username>")
				continue
			}
			server.mu.Lock()
			_, exists := server.clients[arg]
			server.mu.Unlock()

			if !exists {
				fmt.Println("Error: User does not exist.")
			} else {
				activeUser = arg
				fmt.Printf("Active user changed to: %s\n", activeUser)
			}

		case "/send":
			if arg == "" {
				fmt.Println("Usage: /send <message>")
				continue
			}
			if activeUser == "" {
				fmt.Println("Error: No active user selected. Use /select <username> first.")
				continue
			}
			server.MessagePipe <- Message{
				Sender: activeUser,
				Type:   "msg",
				Text:   fmt.Sprintf("[%s]: %s", activeUser, arg),
			}
			fmt.Println("Message sent.")

		case "/remove":
			if arg == "" {
				fmt.Println("Usage: /remove <username>")
				continue
			}
			server.mu.Lock()
			_, exists := server.clients[arg]
			server.mu.Unlock()

			if !exists {
				fmt.Println("Error: User does not exist.")
			} else {
				server.LeavePipe <- arg
				fmt.Printf("User '%s' removed.\n", arg)
				if activeUser == arg {
					activeUser = ""
					fmt.Println("Active user was removed. Please /select another user.")
				}
			}

		case "/exit":
			server.Stop()
			return

		case "/help":
			printMenu()

		default:
			fmt.Println("Unknown command. Type /help for a list of commands.")
		}
	}
}

func printMenu() {
	fmt.Println("=========================================")
	fmt.Println("   Concurrent Chat System (Terminal UI)  ")
	fmt.Println("=========================================")
	fmt.Println(" Commands:")
	fmt.Println("  /create <name>   - Create a new connected user")
	fmt.Println("  /list            - List all connected users")
	fmt.Println("  /select <name>   - Select which user to act as")
	fmt.Println("  /send <msg>      - Send a message as the selected user")
	fmt.Println("  /remove <name>   - Remove a user from the server")
	fmt.Println("  /help            - Show this menu again")
	fmt.Println("  /exit            - Shut down and exit")
	fmt.Println("=========================================")
}
