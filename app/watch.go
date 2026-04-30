package main

func (s *Server) handleWATCHCmd(client *Client, message []string) (string, error) {
	if client.queueCmds {
		return "-ERR WATCH inside MULTI is not allowed\r\n", nil
	}

	if client.watchedKeys == nil {

		client.watchedKeys = make(map[string]int64)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for i := 4; i < len(message); i += 2 {
		if message[i] == "" {
			break
		}
		client.watchedKeys[message[i]] = s.keyVersions[message[i]]
	}

	return "+OK\r\n", nil
}

func (s *Server) markKeyModified(key string) {
	if s.keyVersions == nil {
		s.keyVersions = make(map[string]int64)
	}
	s.keyVersions[key]++
}

func (s *Server) handleUNWATCHCmd(client *Client, message []string) (string, error) {
	if client.queueCmds {
		return "-ERR UNWATCH inside MULTI is not allowed\r\n", nil
	}

	if client.watchedKeys == nil {
		return "+OK\r\n", nil
	}

	// Clear the watched keys for this client connection.
	for k := range client.watchedKeys {
		delete(client.watchedKeys, k)
	}
	return "+OK\r\n", nil
}
