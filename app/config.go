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
	value, ok := defaultConfigValue(option)
	if !ok {
		return "*0\r\n", nil
	}

	return fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(option), option, len(value), value), nil
}

func defaultConfigValue(option string) (string, bool) {
	switch option {
	case "dir":
		dir, err := os.Getwd()
		if err != nil {
			return "", false
		}
		return dir, true
	case "appendonly":
		return "no", true
	case "appenddirname":
		return "appendonlydir", true
	case "appendfilename":
		return "appendonly.aof", true
	case "appendfsync":
		return "everysec", true
	default:
		return "", false
	}
}
