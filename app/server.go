package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type Client struct {
	conn      net.Conn
	cmdList   [][]string
	queueCmds bool
}

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
		go handleConn(&Client{conn: conn})
	}
}

// handleConn handles a connection and responds to the messages being
// written in it.
func handleConn(client *Client) {
	defer client.conn.Close()
	for {
		// Read message from the connection and check if its PING.
		data := make([]byte, 1024) // 1 KB buffer.
		n, err := client.conn.Read(data)
		if err != nil {
			fmt.Println("Error reading message from connection: ", err.Error())
			os.Exit(1)
		}

		// Need to parse input and extract arguments from it.
		m := string(data[:n])
		message := strings.Split(m, "\r\n")

		resp, err := invokeCmdHandler(client, message)
		if err != nil {
			fmt.Println("Error while invoking handler: ", err.Error())
			os.Exit(1)
		}

		_, err = client.conn.Write([]byte(resp))
		if err != nil {
			fmt.Println("Error writing message into connection: ", err.Error())
			os.Exit(1)
		}
	}
}

// invokeCmdHandler handles invoking the right handler based on
// the command passed by the user.
func invokeCmdHandler(client *Client, message []string) (string, error) {
	resp := ""
	var err error

	switch message[2] {
	case "ECHO":
		resp, err = handleEchoCmd(message)
		if err != nil {
			return "", fmt.Errorf("error calling ECHO cmd: %w", err)
		}
	case "SET":
		resp, err = handleSetCmd(client, message)
		if err != nil {
			return "", fmt.Errorf("error calling SET cmd: %w", err)
		}
	case "GET":
		resp, err = handleGetCmd(client, message)
		if err != nil {
			return "", fmt.Errorf("error calling GET cmd: %w", err)
		}
	case "INCR":
		resp, err = handleIncrCmd(client, message)
		if err != nil {
			return "", fmt.Errorf("error calling INCR cmd: %w", err)
		}
	case "MULTI":
		resp, err = handleMultiCmd(client)
		if err != nil {
			return "", fmt.Errorf("error calling MULTI cmd: %w", err)
		}
	case "EXEC":
		resp, err = handleExecCmd(client)
		if err != nil {
			return "", fmt.Errorf("error calling EXEC cmd: %w", err)
		}
	case "DISCARD":
		resp, err = handleDiscardCmd(client)
		if err != nil {
			return "", fmt.Errorf("error calling DISCARD cmd: %w", err)
		}
	default:
		return "+PONG\r\n", nil
	}
	return resp, nil
}

// handleDiscardCmd handles the redis DISCARD command.
func handleDiscardCmd(client *Client) (string, error) {
	// Error out if the DISCARD command is called without MULTI being
	// invoked first.
	if !client.queueCmds {
		return "-ERR DISCARD without MULTI\r\n", nil
	}

	// Discard all the queued commands and set
	// queueCmds to false.
	client.queueCmds = false
	client.cmdList = nil

	return "+OK\r\n", nil
}

// handleExecCmd handles the redis EXEC command.
func handleExecCmd(client *Client) (string, error) {
	// Error out if the EXEC command is called without MULTI being
	// invoked first.
	if !client.queueCmds {
		return "-ERR EXEC without MULTI\r\n", nil
	}

	queuedCmds := client.cmdList
	client.queueCmds = false
	client.cmdList = nil

	resp := fmt.Sprintf("*%d\r\n", len(queuedCmds))

	// Execute all the queued commands one by one.
	for _, msg := range queuedCmds {
		cmdResp, err := invokeCmdHandler(client, msg)
		if err != nil {
			return "", err
		}

		resp += cmdResp
	}

	return resp, nil
}

// handleMultiCmd handles the MULTI command.
// We need to queue the next commands now and execute them
// sequentially once the EXEC command is passed.
func handleMultiCmd(client *Client) (string, error) {
	client.queueCmds = true
	client.cmdList = make([][]string, 0)

	return "+OK\r\n", nil
}

// handleIncrCmd handles the INCR command.
func handleIncrCmd(client *Client, message []string) (string, error) {
	if client.queueCmds {
		client.cmdList = append(client.cmdList, message)
		return "+QUEUED\r\n", nil
	}

	key := message[4]
	resp := ""

	val, ok := kv[key]
	if ok {
		intVal, err := strconv.Atoi(val.value)
		if err != nil {
			resp = "-ERR value is not an integer or out of range\r\n"
		}

		if resp == "" {
			kv[key] = ValueEntry{
				value:     strconv.Itoa(intVal + 1),
				hasExpiry: val.hasExpiry,
				expiresAt: val.expiresAt,
			}
			resp = fmt.Sprintf(":%s\r\n", kv[key].value)
		}
	} else {
		kv[key] = ValueEntry{
			value: strconv.Itoa(1),
		}
		resp = ":1\r\n"
	}

	return resp, nil
}

// handleSetCmd handles the SET command.
func handleSetCmd(client *Client, message []string) (string, error) {
	if client.queueCmds {
		client.cmdList = append(client.cmdList, message)
		return "+QUEUED\r\n", nil
	}

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
	return "+OK\r\n", nil
}

// handleGetCmd gets the value of a key and returns it.
func handleGetCmd(client *Client, message []string) (string, error) {
	if client.queueCmds {
		client.cmdList = append(client.cmdList, message)
		return "+QUEUED\r\n", nil
	}

	if val, ok := kv[message[4]]; ok {
		if val.hasExpiry && !time.Now().After(val.expiresAt) || !val.hasExpiry {
			// $<length>\r\n<data>\r\n
			return fmt.Sprintf("$%d\r\n%s\r\n", len(kv[message[4]].value), kv[message[4]].value), nil
		}
	}

	return "$-1\r\n", nil
}

// handleEchoCmd handles the ECHO command.
func handleEchoCmd(message []string) (string, error) {
	// Bulk string format: $<length>\r\n<data>\r\n
	return fmt.Sprintf("$%d\r\n%s\r\n", len(message[4]), message[4]), nil
}
