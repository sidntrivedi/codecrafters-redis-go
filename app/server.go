package main

import (
	"fmt"
	"net"
	"os"
	"strings"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

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

		// Need to parse input and extract second argument from it.
		m := string(data[:n])
		message := strings.Split(m, "\r\n")

		if message[2] == "ECHO" {
			// Bulk string format: $<length>\r\n<data>\r\n
			resp := fmt.Sprintf("$%d\r\n%s\r\n", len(message[4]), message[4])
			_, err = conn.Write([]byte(resp))
			if err != nil {
				fmt.Println("Error writing message into connection: ", err.Error())
				os.Exit(1)
			}
			return
		}

		_, err = conn.Write([]byte("+PONG\r\n"))
		if err != nil {
			fmt.Println("Error writing message into connection: ", err.Error())
			os.Exit(1)
		}
	}
}
