package main

import "strconv"

type coordinates struct {
	latitude  float64
	longitude float64
}

// handleGeoAddCmd handles the GEOADD redis cmd.
func (s *Server) handleGEOADDCmd(client *Client, message []string) (string, error) {
	if client.queueCmds {
		client.cmdList = append(client.cmdList, message)
		return "+QUEUED\r\n", nil
	}

	key, longitude, latitude, member := message[4], message[6], message[8], message[10]
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.geospatialEntry == nil {
		s.geospatialEntry = make(map[string]map[string]coordinates)
	}
	if s.geospatialEntry[key] == nil {
		s.geospatialEntry[key] = make(map[string]coordinates)
	}

	lat, _ := strconv.ParseFloat(latitude, 64)
	long, _ := strconv.ParseFloat(longitude, 64)

	s.geospatialEntry[key][member] = coordinates{
		latitude:  lat,
		longitude: long,
	}

	return ":1\r\n", nil
}
