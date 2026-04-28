package main

import (
	"fmt"
	"strconv"
)

const (
	minGeoLongitude = -180.0
	maxGeoLongitude = 180.0
	minGeoLatitude  = -85.05112878
	maxGeoLatitude  = 85.05112878
)

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
	long, err := strconv.ParseFloat(longitude, 64)
	if err != nil {
		return fmt.Sprintf("-ERR invalid longitude,latitude pair %s,%s\r\n", longitude, latitude), nil
	}

	lat, err := strconv.ParseFloat(latitude, 64)
	if err != nil {
		return fmt.Sprintf("-ERR invalid longitude,latitude pair %s,%s\r\n", longitude, latitude), nil
	}

	if long < minGeoLongitude || long > maxGeoLongitude || lat < minGeoLatitude || lat > maxGeoLatitude {
		return fmt.Sprintf("-ERR invalid longitude,latitude pair %f,%f\r\n", long, lat), nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.geospatialEntry == nil {
		s.geospatialEntry = make(map[string]map[string]coordinates)
	}
	if s.geospatialEntry[key] == nil {
		s.geospatialEntry[key] = make(map[string]coordinates)
	}
	if s.sset[key] == nil {
		s.sset[key] = make(map[string]float64)
	}

	s.geospatialEntry[key][member] = coordinates{
		latitude:  lat,
		longitude: long,
	}

	_, alreadyExists := s.sset[key][member]
	s.sset[key][member] = 0
	if alreadyExists {
		return ":0\r\n", nil
	}

	return ":1\r\n", nil
}
