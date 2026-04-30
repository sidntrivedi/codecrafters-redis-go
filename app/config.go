package main

import (
	"fmt"
	"os"
	"strings"
)

func (s *Server) handleCONFIGCmd(client *Client, message []string) (string, error) {
	if client.queueCmds {
		client.cmdList = append(client.cmdList, message)
		return "+QUEUED\r\n", nil
	}

	if len(message) <= 6 || strings.ToUpper(message[4]) != "GET" {
		return "-ERR unsupported CONFIG subcommand\r\n", nil
	}

	option := message[6]
	if s.config == nil {
		s.config = loadConfig(nil)
	}

	value, ok := s.config[option]
	if !ok {
		return "*0\r\n", nil
	}

	return fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(option), option, len(value), value), nil
}

func loadConfig(args []string) map[string]string {
	dir, err := os.Getwd()
	if err != nil {
		dir = ""
	}

	config := map[string]string{
		"dir":            dir,
		"appendonly":     "no",
		"appenddirname":  "appendonlydir",
		"appendfilename": "appendonly.aof",
		"appendfsync":    "everysec",
	}

	for i := 0; i < len(args); i++ {
		if !strings.HasPrefix(args[i], "--") {
			continue
		}
		name := args[i][2:]

		if key, value, hasValue := strings.Cut(name, "="); hasValue {
			if _, ok := config[key]; ok {
				config[key] = value
			}
			continue
		}

		if _, ok := config[name]; ok && i+1 < len(args) {
			config[name] = args[i+1]
			i++
		}
	}

	return config
}
