package main

import (
	"fmt"
	"math"
	"strconv"
)

const (
	MIN_LATITUDE  = -85.05112878
	MAX_LATITUDE  = 85.05112878
	MIN_LONGITUDE = -180.0
	MAX_LONGITUDE = 180.0

	LATITUDE_RANGE  = MAX_LATITUDE - MIN_LATITUDE
	LONGITUDE_RANGE = MAX_LONGITUDE - MIN_LONGITUDE
)

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

	if long < MIN_LONGITUDE || long > MAX_LONGITUDE || lat < MIN_LATITUDE || lat > MAX_LATITUDE {
		return fmt.Sprintf("-ERR invalid longitude,latitude pair %f,%f\r\n", long, lat), nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.sset == nil {
		s.sset = make(map[string]map[string]float64)
	}
	if s.sset[key] == nil {
		s.sset[key] = make(map[string]float64)
	}

	_, alreadyExists := s.sset[key][member]
	score := encode(lat, long)
	s.sset[key][member] = float64(score)
	if alreadyExists {
		return ":0\r\n", nil
	}

	return ":1\r\n", nil
}

// ===========================
// Encoding logic for latitudes
// and longitude.
// Taken from: https://github.com/codecrafters-io/redis-geocoding-algorithm/blob/main/go/encode.go
func spreadInt32ToInt64(v uint32) uint64 {
	result := uint64(v)
	result = (result | (result << 16)) & 0x0000FFFF0000FFFF
	result = (result | (result << 8)) & 0x00FF00FF00FF00FF
	result = (result | (result << 4)) & 0x0F0F0F0F0F0F0F0F
	result = (result | (result << 2)) & 0x3333333333333333
	result = (result | (result << 1)) & 0x5555555555555555
	return result
}

func interleave(x, y uint32) uint64 {
	xSpread := spreadInt32ToInt64(x)
	ySpread := spreadInt32ToInt64(y)
	yShifted := ySpread << 1
	return xSpread | yShifted
}

func encode(latitude, longitude float64) uint64 {
	// Normalize to the range 0-2^26
	normalizedLatitude := math.Pow(2, 26) * (latitude - MIN_LATITUDE) / LATITUDE_RANGE
	normalizedLongitude := math.Pow(2, 26) * (longitude - MIN_LONGITUDE) / LONGITUDE_RANGE

	// Truncate to integers
	latInt := uint32(normalizedLatitude)
	lonInt := uint32(normalizedLongitude)

	return interleave(latInt, lonInt)
}

// ===========================
