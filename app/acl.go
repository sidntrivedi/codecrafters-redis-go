package main

func (s *Server) handleACLCmd(client *Client, message []string) (string, error) {
	return "$7\r\ndefault\r\n", nil
}
