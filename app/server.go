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

var (
	kv              map[string]ValueEntry
	multiCmdInvoked bool
	cmdList         [][]string // cmdList stores the commands list when MULTI is used.
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")
	kv = make(map[string]ValueEntry)
	multiCmdInvoked = false

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

		resp, err := invokeCmdHandler(conn, message)
		if err != nil {
			fmt.Println("Error while invoking handler: ", err.Error())
			os.Exit(1)
		}

		_, err = conn.Write([]byte(resp))
		if err != nil {
			fmt.Println("Error writing message into connection: ", err.Error())
			os.Exit(1)
		}
	}
}

// invokeCmdHandler handles invoking the right handler based on
// the command passed by the user.
func invokeCmdHandler(conn net.Conn, message []string) (string, error) {
	resp := ""
	var err error

	switch message[2] {
	case "ECHO":
		resp, err = handleEchoCmd(conn, message)
		if err != nil {
			return "", fmt.Errorf("error calling ECHO cmd: %w", err)
		}
	case "SET":
		resp, err = handleSetCmd(conn, message)
		if err != nil {
			return "", fmt.Errorf("error calling SET cmd: %w", err)
		}
	case "GET":
		resp, err = handleGetCmd(conn, message)
		if err != nil {
			return "", fmt.Errorf("error calling GET cmd: %w", err)
		}
	case "INCR":
		resp, err = handleIncrCmd(conn, message)
		if err != nil {
			return "", fmt.Errorf("error calling INCR cmd: %w", err)
		}
	case "MULTI":
		resp, err = handleMultiCmd(conn)
		if err != nil {
			return "", fmt.Errorf("error calling MULTI cmd: %w", err)
		}
	case "EXEC":
		resp, err = handleExecCmd(conn)
		if err != nil {
			return "", fmt.Errorf("error calling EXEC cmd: %w", err)
		}
	default:
		if _, err := conn.Write([]byte("+PONG\r\n")); err != nil {
			return "", err
		}
	}
	return resp, nil
}

func handleExecCmd(conn net.Conn) (string, error) {
	// Error out if the EXEC command is called without MULTI being
	// invoked first.
	if !multiCmdInvoked {
		return "-ERR EXEC without MULTI\r\n", nil
	}

	queuedCmds := cmdList
	multiCmdInvoked = false
	cmdList = nil

	resp := fmt.Sprintf("*%d\r\n", len(queuedCmds))

	// Execute all the queued commands one by one.
	for _, msg := range queuedCmds {
		cmdResp, err := invokeCmdHandler(conn, msg)
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
func handleMultiCmd(conn net.Conn) (string, error) {
	multiCmdInvoked = true
	cmdList = make([][]string, 0)

	return "+OK\r\n", nil
}

// handleIncrCmd handles the INCR command.
func handleIncrCmd(conn net.Conn, message []string) (string, error) {
	if multiCmdInvoked {
		cmdList = append(cmdList, message)
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
func handleSetCmd(conn net.Conn, message []string) (string, error) {
	if multiCmdInvoked {
		cmdList = append(cmdList, message)
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
func handleGetCmd(conn net.Conn, message []string) (string, error) {
	if multiCmdInvoked {
		cmdList = append(cmdList, message)
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
func handleEchoCmd(conn net.Conn, message []string) (string, error) {
	// Bulk string format: $<length>\r\n<data>\r\n
	return fmt.Sprintf("$%d\r\n%s\r\n", len(message[4]), message[4]), nil
}
