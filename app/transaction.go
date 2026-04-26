package main

import "fmt"

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
