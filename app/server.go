package main

import (
	"fmt"
	"net"
	"os"
	"strings"
)

var kv map[string]string

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")
	kv = make(map[string]string)

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConn(conn)
	}
}

// handleConn handles a connection and responds to the messages being
// written in it.
func handleConn(conn net.Conn) {
	defer conn.Close()
	for {
		// Read message from the connection and check if its PING.
		data := make([]byte, 1024) // 1 KB buffer.
		n, err := conn.Read(data)
		if err != nil {
			fmt.Println("Error reading message from connection: ", err.Error())
			os.Exit(1)
		}

		// Need to parse input and extract arguments from it.
		m := string(data[:n])
		message := strings.Split(m, "\r\n")

		switch message[2] {
		case "ECHO":
			handleEchoCmd(conn, message)
		case "SET":
			handleSetCmd(conn, message)
		case "GET":
			handleGetCmd(conn, message)
		default:
			_, err = conn.Write([]byte("+PONG\r\n"))
			if err != nil {
				fmt.Println("Error writing message into connection: ", err.Error())
				os.Exit(1)
			}
		}
	}
}

// handleSetCmd handles the SET command.
func handleSetCmd(conn net.Conn, message []string) error {
	kv[message[4]] = message[6]
	_, err := conn.Write([]byte("+OK\r\n"))
	if err != nil {
		fmt.Println("Error writing message into connection: ", err.Error())
		os.Exit(1)
	}
	return nil
}

// handleGetCmd gets the value of a key and returns it.
func handleGetCmd(conn net.Conn, message []string) error {
	resp := ""
	if _, ok := kv[message[4]]; ok {
		// $<length>\r\n<data>\r\n
		resp = fmt.Sprintf("$%d\r\n%s\r\n", len(kv[message[4]]), kv[message[4]])
	}

	if resp == "" {
		resp = "$-1\r\n"
	}

	_, err := conn.Write([]byte(resp))
	if err != nil {
		fmt.Println("Error writing message into connection: ", err.Error())
		os.Exit(1)
	}
	return nil
}

// handleEchoCmd handles the ECHO command.
func handleEchoCmd(conn net.Conn, message []string) (err error) {
	// Bulk string format: $<length>\r\n<data>\r\n
	resp := fmt.Sprintf("$%d\r\n%s\r\n", len(message[4]), message[4])
	_, err = conn.Write([]byte(resp))
	if err != nil {
		fmt.Println("Error writing message into connection: ", err.Error())
		os.Exit(1)
	}
	return nil
}
