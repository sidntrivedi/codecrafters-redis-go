package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

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
	key := message[4]

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
	s.mu.Lock()
	defer s.mu.Unlock()

	s.kv[key] = ValueEntry{
		valueType: TypeString,
		strValue:  message[6],
		hasExpiry: hasTimeout,
		expiresAt: time.Now().Add(timeout),
	}
	s.markKeyModified(key)
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
