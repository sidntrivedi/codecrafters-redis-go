package main

import (
	"fmt"
	"strconv"
	"time"
)

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
