package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type ValueEntry struct {
	value     string
	expiresAt time.Time
	hasExpiry bool
}

var kv map[string]ValueEntry

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")
	kv = make(map[string]ValueEntry)

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
		case "INCR":
			handleIncrCmd(conn, message)
		default:
			_, err = conn.Write([]byte("+PONG\r\n"))
			if err != nil {
				fmt.Println("Error writing message into connection: ", err.Error())
				os.Exit(1)
			}
		}
	}
}

func handleIncrCmd(conn net.Conn, message []string) error {
	key := message[4]
	resp := ""

	val, ok := kv[key]
	if ok {
		intVal, _ := strconv.Atoi(val.value)
		kv[key] = ValueEntry{
			value:     strconv.Itoa(intVal + 1),
			hasExpiry: val.hasExpiry,
			expiresAt: val.expiresAt,
		}
		resp = fmt.Sprintf(":%s\r\n", kv[key].value)
	} else {
		kv[key] = ValueEntry{
			value: strconv.Itoa(1),
		}
		resp = ":1\r\n"
	}

	_, err := conn.Write([]byte(resp))
	if err != nil {
		fmt.Println("Error writing message into connection: ", err.Error())
		os.Exit(1)
	}
	return nil
}

// handleSetCmd handles the SET command.
func handleSetCmd(conn net.Conn, message []string) error {
	var timeout time.Duration
	hasTimeout := false

	// Check for EX and PX arguments with the SET command.
	if len(message) > 8 && strings.ToLower(message[8]) == "ex" {
		hasTimeout = true
		t, _ := strconv.Atoi(message[10])
		timeout = time.Duration(t) * time.Second
	} else if len(message) > 8 && strings.ToLower(message[8]) == "px" {
		hasTimeout = true
		t, _ := strconv.Atoi(message[10])
		timeout = time.Duration(t) * time.Millisecond
	}

	// Write the key value pair to map.
	kv[message[4]] = ValueEntry{
		value:     message[6],
		hasExpiry: hasTimeout,
		expiresAt: time.Now().Add(timeout),
	}
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
	if val, ok := kv[message[4]]; ok {
		if val.hasExpiry && !time.Now().After(val.expiresAt) || !val.hasExpiry {
			// $<length>\r\n<data>\r\n
			resp = fmt.Sprintf("$%d\r\n%s\r\n", len(kv[message[4]].value), kv[message[4]].value)
		}
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
