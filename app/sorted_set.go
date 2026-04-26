package main

import (
	"fmt"
	"sort"
	"strconv"
)

type sortedSetEntry struct {
	member string
	score  float64
}

func sortedSetEntries(set map[string]float64) []sortedSetEntry {
	entries := make([]sortedSetEntry, 0, len(set))
	for member, score := range set {
		entries = append(entries, sortedSetEntry{member: member, score: score})
	}

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].score != entries[j].score {
			return entries[i].score < entries[j].score
		}
		return entries[i].member < entries[j].member
	})

	return entries
}

// handleZRANKCmd handles the ZRANK redis cmd.
func (s *Server) handleZRANKCmd(client *Client, message []string) (string, error) {
	if client.queueCmds {
		client.cmdList = append(client.cmdList, message)
		return "+QUEUED\r\n", nil
	}

	set := message[4]
	memberName := message[6]

	s.mu.Lock()
	defer s.mu.Unlock()

	// In case the member doesn't exist, return null string.
	if _, ok := s.sset[set][memberName]; !ok {
		return "$-1\r\n", nil
	}

	for idx, entry := range sortedSetEntries(s.sset[set]) {
		if entry.member == memberName {
			return fmt.Sprintf(":%d\r\n", idx), nil
		}
	}

	return "$-1\r\n", nil
}

// handleZRANGECmd handles the ZRANGE redis cmd.
func (s *Server) handleZRANGECmd(client *Client, message []string) (string, error) {
	if client.queueCmds {
		client.cmdList = append(client.cmdList, message)
		return "+QUEUED\r\n", nil
	}

	set := message[4]
	start, err := strconv.Atoi(message[6])
	if err != nil {
		return "", err
	}

	if start < 0 {
		start = len(s.sset[set]) + start
		if start < 0 {
			start = 0
		}
	}

	stop, err := strconv.Atoi(message[8])
	if err != nil {
		return "", err
	}

	if stop < 0 {
		stop = len(s.sset[set]) + stop
		if stop < 0 {
			stop = 0
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	entries := sortedSetEntries(s.sset[set])
	if len(entries) == 0 {
		return "*0\r\n", nil
	}

	if start >= len(entries) || start > stop {
		return "*0\r\n", nil
	}

	if stop >= len(entries) {
		stop = len(entries) - 1
	}

	resp := fmt.Sprintf("*%d\r\n", (stop-start)+1)
	for i := start; i <= stop; i++ {
		member := entries[i].member
		resp += fmt.Sprintf("$%d\r\n%s\r\n", len(member), member)
	}

	return resp, nil
}

// handleZCARDcmd handles the ZCARD redis cmd.
func (s *Server) handleZCARDcmd(client *Client, message []string) (string, error) {
	if client.queueCmds {
		client.cmdList = append(client.cmdList, message)
		return "+QUEUED\r\n", nil
	}

	set := message[4]

	s.mu.Lock()
	defer s.mu.Unlock()

	return fmt.Sprintf(":%d\r\n", len(s.sset[set])), nil
}

// handleZSCOREcmd handles the ZSCORE redis cmd.
func (s *Server) handleZSCOREcmd(client *Client, message []string) (string, error) {
	if client.queueCmds {
		client.cmdList = append(client.cmdList, message)
		return "+QUEUED\r\n", nil
	}

	set := message[4]
	member := message[6]

	s.mu.Lock()
	defer s.mu.Unlock()

	score, ok := s.sset[set][member]
	if !ok {
		return "$-1\r\n", nil
	}

	scoreStr := strconv.FormatFloat(score, 'f', -1, 64)
	return fmt.Sprintf("$%d\r\n%s\r\n", len(scoreStr), scoreStr), nil
}

// handleZADDcmd handles the ZADD redis cmd.
func (s *Server) handleZADDcmd(client *Client, message []string) (string, error) {
	if client.queueCmds {
		client.cmdList = append(client.cmdList, message)
		return "+QUEUED\r\n", nil
	}

	set, value, name := message[4], message[6], message[8]
	s.mu.Lock()
	defer s.mu.Unlock()

	// convert score to float64.
	val, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return "", err
	}

	if s.sset[set] == nil {
		s.sset[set] = make(map[string]float64)
	}

	_, alreadyExists := s.sset[set][name]
	s.sset[set][name] = val
	if alreadyExists {
		return ":0\r\n", nil
	}

	return ":1\r\n", nil
}
