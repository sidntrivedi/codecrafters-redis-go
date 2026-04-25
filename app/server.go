package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type ValueType string

const (
	TypeString ValueType = "string"
	TypeList   ValueType = "list"
)

// allowedSubscribeCmds contains the list of commands
// that are allowed when redis is in subscribe mode.
var allowedSubscribeCmds = map[string]struct{}{
	"SUBSCRIBE":    {},
	"UNSUBSCRIBE":  {},
	"PSUBSCRIBE":   {},
	"PUNSUBSCRIBE": {},
	"PING":         {},
	"QUIT":         {},
}

type Server struct {
	mu      sync.Mutex
	kv      map[string]ValueEntry
	waiters map[string][]chan string
}

type Client struct {
	conn          net.Conn
	cmdList       [][]string
	queueCmds     bool
	subscribeMode SubscribeMode
}

type SubscribeMode struct {
	enabled  bool
	channels []string
}

type ValueEntry struct {
	valueType ValueType
	strValue  string
	listValue []string
	expiresAt time.Time
	hasExpiry bool
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")
	server := &Server{
		kv:      make(map[string]ValueEntry),
		waiters: make(map[string][]chan string),
	}

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
		go server.handleConn(&Client{conn: conn})
	}
}

// handleConn handles a connection and responds to the messages being
// written in it.
func (s *Server) handleConn(client *Client) {
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

		resp, err := s.invokeCmdHandler(client, message)
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
func (s *Server) invokeCmdHandler(client *Client, message []string) (string, error) {
	resp := ""
	var err error
	cmd := strings.ToUpper(message[2])

	if client.subscribeMode.enabled && !isAllowedInSubscribeMode(cmd) {
		return fmt.Sprintf("-ERR Can't execute '%s': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT are allowed in this context\r\n", cmd), nil
	}

	switch cmd {
	case "ECHO":
		resp, err = handleEchoCmd(message)
		if err != nil {
			return "", fmt.Errorf("error calling ECHO cmd: %w", err)
		}
	case "SET":
		resp, err = s.handleSetCmd(client, message)
		if err != nil {
			return "", fmt.Errorf("error calling SET cmd: %w", err)
		}
	case "GET":
		resp, err = s.handleGetCmd(client, message)
		if err != nil {
			return "", fmt.Errorf("error calling GET cmd: %w", err)
		}
	case "INCR":
		resp, err = s.handleIncrCmd(client, message)
		if err != nil {
			return "", fmt.Errorf("error calling INCR cmd: %w", err)
		}
	case "MULTI":
		resp, err = handleMultiCmd(client)
		if err != nil {
			return "", fmt.Errorf("error calling MULTI cmd: %w", err)
		}
	case "EXEC":
		resp, err = s.handleExecCmd(client)
		if err != nil {
			return "", fmt.Errorf("error calling EXEC cmd: %w", err)
		}
	case "DISCARD":
		resp, err = handleDiscardCmd(client)
		if err != nil {
			return "", fmt.Errorf("error calling DISCARD cmd: %w", err)
		}
	case "RPUSH":
		resp, err = s.handleRPUSHCmd(client, message)
		if err != nil {
			return "", fmt.Errorf("error calling RPUSH cmd: %w", err)
		}
	case "LPUSH":
		resp, err = s.handleLPUSHCmd(client, message)
		if err != nil {
			return "", fmt.Errorf("error calling LPUSH cmd: %w", err)
		}
	case "LRANGE":
		resp, err = s.handleLRANGECmd(client, message)
		if err != nil {
			return "", fmt.Errorf("error calling RPUSH cmd: %w", err)
		}
	case "LLEN":
		resp, err = s.handleLLENCmd(client, message)
		if err != nil {
			return "", fmt.Errorf("error calling LLEN cmd: %w", err)
		}
	case "LPOP":
		resp, err = s.handleLPOPCmd(client, message)
		if err != nil {
			return "", fmt.Errorf("error calling LPOP cmd: %w", err)
		}
	case "BLPOP":
		resp, err = s.handleBLPOPCmd(client, message)
		if err != nil {
			return "", fmt.Errorf("error calling LPOP cmd: %w", err)
		}
	case "SUBSCRIBE":
		resp, err = s.handleSubscribeCmd(client, message)
		if err != nil {
			return "", fmt.Errorf("error calling LPOP cmd: %w", err)
		}
	default:
		if client.subscribeMode.enabled {
			return "*2\r\n$4\r\npong\r\n$0\r\n\r\n", nil
		}
		return "+PONG\r\n", nil
	}
	return resp, nil
}

func isAllowedInSubscribeMode(cmd string) bool {
	_, ok := allowedSubscribeCmds[cmd]
	return ok
}

// handleSubscribeCmd handles the SUBSCRIBE redis cmd.
func (s *Server) handleSubscribeCmd(client *Client, message []string) (string, error) {
	if client.queueCmds {
		client.cmdList = append(client.cmdList, message)
		return "+QUEUED\r\n", nil
	}

	// Add the channels passed to the client channels slice.
	s.mu.Lock()
	defer s.mu.Unlock()

	channel := message[4]
	client.subscribeMode = SubscribeMode{
		enabled:  true,
		channels: append(client.subscribeMode.channels, channel),
	}

	return fmt.Sprintf("*3\r\n$9\r\nsubscribe\r\n$%d\r\n%s\r\n:%d\r\n", len(channel), channel, len(client.subscribeMode.channels)), nil
}

// handleLPUSHCmd handles the LPUSH redis cmd.
func (s *Server) handleLPUSHCmd(client *Client, message []string) (string, error) {
	if client.queueCmds {
		client.cmdList = append(client.cmdList, message)
		return "+QUEUED\r\n", nil
	}

	key := message[4]
	values := extractListValues(message)

	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.kv[key]
	if ok && isExpired(entry) {
		delete(s.kv, key)
		ok = false
	}

	if ok && entry.valueType != TypeList {
		return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n", nil
	}

	if !ok {
		entry = ValueEntry{
			valueType: TypeList,
		}
	}

	newList := make([]string, 0, len(values)+len(entry.listValue))
	for i := len(values) - 1; i >= 0; i-- {
		newList = append(newList, values[i])
	}
	newList = append(newList, entry.listValue...)
	entry.listValue = newList
	entry.valueType = TypeList
	respLen := len(entry.listValue)
	s.deliverWaitingBLPOP(key, &entry)
	if len(entry.listValue) == 0 {
		delete(s.kv, key)
	} else {
		s.kv[key] = entry
	}

	return fmt.Sprintf(":%d\r\n", respLen), nil
}

// handleLRANGECmd handles the LRANGE redis command.
func (s *Server) handleLRANGECmd(client *Client, message []string) (string, error) {
	if client.queueCmds {
		client.cmdList = append(client.cmdList, message)
		return "+QUEUED\r\n", nil
	}

	key := message[4]
	start, _ := strconv.Atoi(message[6])
	stop, _ := strconv.Atoi(message[8])
	// If the list doesn't exist, an empty array is returned.
	entry, ok := s.kv[key]
	if !ok {
		return "*0\r\n", nil
	}

	// Handle the range if there are negative indexes.
	if start < 0 {
		start = len(entry.listValue) + start
		// If a negative index is out of range (e.g., -6 on a list of length 5),
		// it should be treated as 0 (the start of the list).
		if start < 0 {
			start = 0
		}
	}

	if stop < 0 {
		stop = len(entry.listValue) + stop
		// If a negative index is out of range (e.g., -6 on a list of length 5),
		// it should be treated as 0 (the start of the list).
		if stop < 0 {
			stop = 0
		}
	}

	// If the start index is greater than the stop index, an empty array is returned.
	if start > stop {
		return "*0\r\n", nil
	}

	// If the start index is greater than or equal to the list's length, an empty array is returned.
	if start > len(entry.listValue) {
		return "*0\r\n", nil
	}

	// If the stop index is greater than or equal to the list's length, the stop index is treated as the last element.
	if stop > len(entry.listValue) {
		stop = len(entry.listValue) - 1
	}

	resp := fmt.Sprintf("*%d\r\n", (stop-start)+1)
	for i := start; i <= stop; i++ {
		resp = resp + fmt.Sprintf("$%d\r\n%s\r\n", len(entry.listValue[i]), entry.listValue[i])
	}

	return resp, nil
}

// handleLLENCmd handles the LLEN redis command.
func (s *Server) handleLLENCmd(client *Client, message []string) (string, error) {
	if client.queueCmds {
		client.cmdList = append(client.cmdList, message)
		return "+QUEUED\r\n", nil
	}

	key := message[4]
	entry, ok := s.kv[key]
	if !ok {
		return ":0\r\n", nil
	}

	if isExpired(entry) {
		delete(s.kv, key)
		return ":0\r\n", nil
	}

	if entry.valueType != TypeList {
		return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n", nil
	}

	return fmt.Sprintf(":%d\r\n", len(entry.listValue)), nil
}

// handleBLPOPCmd handles the BLPOP redis command.
func (s *Server) handleBLPOPCmd(client *Client, message []string) (string, error) {
	if client.queueCmds {
		client.cmdList = append(client.cmdList, message)
		return "+QUEUED\r\n", nil
	}

	key := message[4]
	timeoutSeconds, err := strconv.ParseFloat(message[6], 64)
	if err != nil || timeoutSeconds < 0 {
		return "-ERR timeout is not a float or out of range\r\n", nil
	}

	s.mu.Lock()
	entry, ok := s.kv[key]

	// If the key is present, pop it and return the response.
	if ok {
		if isExpired(entry) {
			delete(s.kv, key)
			ok = false
		} else if entry.valueType != TypeList {
			s.mu.Unlock()
			return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n", nil
		} else if len(entry.listValue) > 0 {
			value := entry.listValue[0]
			entry.listValue = entry.listValue[1:]

			if len(entry.listValue) == 0 {
				delete(s.kv, key)
			} else {
				s.kv[key] = entry
			}

			s.mu.Unlock()
			return fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(value), value), nil
		}
	}

	// If the key is not present, need to add waiter for it.
	// Update the RPUSH and LPUSH functions to invoke the waiter
	// as soon a key is pushed.
	waiter := make(chan string, 1)
	s.waiters[key] = append(s.waiters[key], waiter)
	s.mu.Unlock()

	if timeoutSeconds == 0 {
		value := <-waiter
		return fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(value), value), nil
	}

	timer := time.NewTimer(time.Duration(timeoutSeconds * float64(time.Second)))
	defer timer.Stop()

	select {
	case value := <-waiter:
		return fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(value), value), nil
	case <-timer.C:
		s.mu.Lock()
		removed := s.removeWaiter(key, waiter)
		s.mu.Unlock()
		if !removed {
			value := <-waiter
			return fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(value), value), nil
		}
		return "*-1\r\n", nil
	}
}

// handleLPOPCmd handles the LPOP redis command.
func (s *Server) handleLPOPCmd(client *Client, message []string) (string, error) {
	if client.queueCmds {
		client.cmdList = append(client.cmdList, message)
		return "+QUEUED\r\n", nil
	}

	key := message[4]
	count := 1
	hasCount := len(message) > 6 && message[6] != ""
	if hasCount {
		parsedCount, err := strconv.Atoi(message[6])
		if err != nil || parsedCount <= 0 {
			return "-ERR value is out of range, must be positive\r\n", nil
		}
		count = parsedCount
	}

	entry, ok := s.kv[key]
	if !ok {
		if hasCount {
			return "*-1\r\n", nil
		}
		return "$-1\r\n", nil
	}

	if isExpired(entry) {
		delete(s.kv, key)
		if hasCount {
			return "*-1\r\n", nil
		}
		return "$-1\r\n", nil
	}

	if entry.valueType != TypeList {
		return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n", nil
	}

	if len(entry.listValue) == 0 {
		delete(s.kv, key)
		if hasCount {
			return "*-1\r\n", nil
		}
		return "$-1\r\n", nil
	}

	if !hasCount {
		value := entry.listValue[0]
		entry.listValue = entry.listValue[1:]

		if len(entry.listValue) == 0 {
			delete(s.kv, key)
		} else {
			s.kv[key] = entry
		}

		return fmt.Sprintf("$%d\r\n%s\r\n", len(value), value), nil
	}

	if count > len(entry.listValue) {
		count = len(entry.listValue)
	}

	poppedValues := entry.listValue[:count]
	entry.listValue = entry.listValue[count:]

	if len(entry.listValue) == 0 {
		delete(s.kv, key)
	} else {
		s.kv[key] = entry
	}

	resp := fmt.Sprintf("*%d\r\n", len(poppedValues))
	for _, value := range poppedValues {
		resp += fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
	}

	return resp, nil
}

// handleRPUSHCmd handles the RPUSH redis cmd.
func (s *Server) handleRPUSHCmd(client *Client, message []string) (string, error) {
	if client.queueCmds {
		client.cmdList = append(client.cmdList, message)
		return "+QUEUED\r\n", nil
	}

	key := message[4]
	values := extractListValues(message)

	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.kv[key]
	if ok && isExpired(entry) {
		delete(s.kv, key)
		ok = false
	}

	if ok && entry.valueType != TypeList {
		return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n", nil
	}

	if !ok {
		entry = ValueEntry{
			valueType: TypeList,
		}
	}

	entry.listValue = append(entry.listValue, values...)
	entry.valueType = TypeList
	respLen := len(entry.listValue)
	s.deliverWaitingBLPOP(key, &entry)
	if len(entry.listValue) == 0 {
		delete(s.kv, key)
	} else {
		s.kv[key] = entry
	}

	return fmt.Sprintf(":%d\r\n", respLen), nil
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
func (s *Server) handleExecCmd(client *Client) (string, error) {
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
		cmdResp, err := s.invokeCmdHandler(client, msg)
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
func (s *Server) handleIncrCmd(client *Client, message []string) (string, error) {
	if client.queueCmds {
		client.cmdList = append(client.cmdList, message)
		return "+QUEUED\r\n", nil
	}

	key := message[4]
	resp := ""

	val, ok := s.kv[key]
	if ok {
		if isExpired(val) {
			delete(s.kv, key)
			ok = false
		}
	}

	if ok {
		if val.valueType != TypeString {
			return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n", nil
		}

		intVal, err := strconv.Atoi(val.strValue)
		if err != nil {
			resp = "-ERR value is not an integer or out of range\r\n"
		}

		if resp == "" {
			s.kv[key] = ValueEntry{
				valueType: TypeString,
				strValue:  strconv.Itoa(intVal + 1),
				hasExpiry: val.hasExpiry,
				expiresAt: val.expiresAt,
			}
			resp = fmt.Sprintf(":%s\r\n", s.kv[key].strValue)
		}
	} else {
		s.kv[key] = ValueEntry{
			valueType: TypeString,
			strValue:  strconv.Itoa(1),
		}
		resp = ":1\r\n"
	}

	return resp, nil
}

// handleSetCmd handles the SET command.
func (s *Server) handleSetCmd(client *Client, message []string) (string, error) {
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
	s.kv[message[4]] = ValueEntry{
		valueType: TypeString,
		strValue:  message[6],
		hasExpiry: hasTimeout,
		expiresAt: time.Now().Add(timeout),
	}
	return "+OK\r\n", nil
}

// handleGetCmd gets the value of a key and returns it.
func (s *Server) handleGetCmd(client *Client, message []string) (string, error) {
	if client.queueCmds {
		client.cmdList = append(client.cmdList, message)
		return "+QUEUED\r\n", nil
	}

	if val, ok := s.kv[message[4]]; ok {
		if isExpired(val) {
			delete(s.kv, message[4])
			return "$-1\r\n", nil
		}
		if val.valueType != TypeString {
			return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n", nil
		}
		// $<length>\r\n<data>\r\n
		return fmt.Sprintf("$%d\r\n%s\r\n", len(val.strValue), val.strValue), nil
	}

	return "$-1\r\n", nil
}

// handleEchoCmd handles the ECHO command.
func handleEchoCmd(message []string) (string, error) {
	// Bulk string format: $<length>\r\n<data>\r\n
	return fmt.Sprintf("$%d\r\n%s\r\n", len(message[4]), message[4]), nil
}

func extractListValues(message []string) []string {
	values := make([]string, 0, (len(message)-6)/2)
	for i := 6; i < len(message); i += 2 {
		if message[i] == "" {
			break
		}
		values = append(values, message[i])
	}

	return values
}

func (s *Server) deliverWaitingBLPOP(key string, entry *ValueEntry) {
	waiters := s.waiters[key]
	for len(waiters) > 0 && len(entry.listValue) > 0 {
		waiter := waiters[0]
		waiters = waiters[1:]
		value := entry.listValue[0]
		entry.listValue = entry.listValue[1:]
		waiter <- value
	}

	if len(waiters) == 0 {
		delete(s.waiters, key)
	} else {
		s.waiters[key] = waiters
	}
}

func (s *Server) removeWaiter(key string, target chan string) bool {
	waiters := s.waiters[key]
	for i, waiter := range waiters {
		if waiter == target {
			waiters = append(waiters[:i], waiters[i+1:]...)
			if len(waiters) == 0 {
				delete(s.waiters, key)
			} else {
				s.waiters[key] = waiters
			}
			return true
		}
	}

	return false
}

func isExpired(val ValueEntry) bool {
	return val.hasExpiry && time.Now().After(val.expiresAt)
}
