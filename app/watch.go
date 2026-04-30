package main

func (s *Server) handleWATCHCmd(client *Client, message []string) (string, error) {
	if client.queueCmds {
		return "-ERR WATCH inside MULTI is not allowed\r\n", nil
	}
	return "+OK\r\n", nil

}
