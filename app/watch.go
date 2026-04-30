package main

func (s *Server) handleWATCHCmd(client *Client, message []string) (string, error) {
	return "+OK\r\n", nil

}
