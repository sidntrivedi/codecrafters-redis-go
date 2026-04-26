package main

import (
	"fmt"
	"os"
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

func isAllowedInSubscribeMode(cmd string) bool {
	_, ok := allowedSubscribeCmds[cmd]
	return ok
}

func (client *Client) writePubSubMessages() {
	for msg := range client.messages {
		resp := fmt.Sprintf("*3\r\n$7\r\nmessage\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(msg.channel), msg.channel, len(msg.message), msg.message)
		_, err := client.conn.Write([]byte(resp))
		if err != nil {
			fmt.Println("Error writing message into connection: ", err.Error())
			os.Exit(1)
		}
	}
}

// handlePublishCmd handles the PUBLISH redis cmd.
func (s *Server) handlePublishCmd(client *Client, message []string) (string, error) {
	if client.queueCmds {
		client.cmdList = append(client.cmdList, message)
		return "+QUEUED\r\n", nil
	}

	channel := message[4]
	msg := message[6]

	for subscriber := range s.subscribers[channel] {
		subscriber.messages <- MessageInfo{
			channel: channel,
			message: msg,
		}
	}

	return fmt.Sprintf(":%d\r\n", s.subscriberCount(channel)), nil
}

// handleSubscribeCmd handles the SUBSCRIBE redis cmd.
func (s *Server) handleSubscribeCmd(client *Client, message []string) (string, error) {
	if client.queueCmds {
		client.cmdList = append(client.cmdList, message)
		return "+QUEUED\r\n", nil
	}

	// Track the subscription from both directions:
	// client -> channels for subscription count, channel -> clients for publish.
	s.mu.Lock()
	defer s.mu.Unlock()

	channel := message[4]
	if client.subscribeMode.channels == nil {
		client.subscribeMode.channels = make(map[string]struct{})
	}
	client.subscribeMode.enabled = true
	client.subscribeMode.channels[channel] = struct{}{}

	if s.subscribers[channel] == nil {
		s.subscribers[channel] = make(map[*Client]struct{})
	}
	s.subscribers[channel][client] = struct{}{}

	return fmt.Sprintf("*3\r\n$9\r\nsubscribe\r\n$%d\r\n%s\r\n:%d\r\n", len(channel), channel, len(client.subscribeMode.channels)), nil
}

// handleUnsubscribeCmd handles the UNSUBSCRIBE redis cmd.
func (s *Server) handleUnsubscribeCmd(client *Client, message []string) (string, error) {
	if client.queueCmds {
		client.cmdList = append(client.cmdList, message)
		return "+QUEUED\r\n", nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	channel := message[4]
	delete(client.subscribeMode.channels, channel)

	if subscribers, ok := s.subscribers[channel]; ok {
		delete(subscribers, client)
		if len(subscribers) == 0 {
			delete(s.subscribers, channel)
		}
	}

	remaining := len(client.subscribeMode.channels)
	if remaining == 0 {
		client.subscribeMode.enabled = false
	}

	return fmt.Sprintf("*3\r\n$11\r\nunsubscribe\r\n$%d\r\n%s\r\n:%d\r\n", len(channel), channel, remaining), nil
}

func (s *Server) subscriberCount(channel string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.subscribers[channel])
}
