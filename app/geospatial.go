package main

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

const (
	MIN_LATITUDE  = -85.05112878
	MAX_LATITUDE  = 85.05112878
	MIN_LONGITUDE = -180.0
	MAX_LONGITUDE = 180.0

	LATITUDE_RANGE  = MAX_LATITUDE - MIN_LATITUDE
	LONGITUDE_RANGE = MAX_LONGITUDE - MIN_LONGITUDE

	EARTH_RADIUS_METERS = 6372797.560856
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

// handleGEOPOSCmd handles the GEOPOS redis cmd.
func (s *Server) handleGEOPOSCmd(client *Client, message []string) (string, error) {
	if client.queueCmds {
		client.cmdList = append(client.cmdList, message)
		return "+QUEUED\r\n", nil
	}

	key := message[4]
	members := make([]string, 0, (len(message)-6)/2)
	for i := 6; i < len(message)-1; i += 2 {
		members = append(members, message[i])
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	resp := fmt.Sprintf("*%d\r\n", len(members))
	for _, member := range members {
		score, ok := s.sset[key][member]
		if !ok {
			resp += "*-1\r\n"
			continue
		}

		coordinates := decode(uint64(score))
		longitude := strconv.FormatFloat(coordinates.Longitude, 'f', -1, 64)
		latitude := strconv.FormatFloat(coordinates.Latitude, 'f', -1, 64)
		resp += fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(longitude), longitude, len(latitude), latitude)
	}

	return resp, nil
}

// handleGEODISTCmd handles the GEODIST redis cmd.
func (s *Server) handleGEODISTCmd(client *Client, message []string) (string, error) {
	if client.queueCmds {
		client.cmdList = append(client.cmdList, message)
		return "+QUEUED\r\n", nil
	}

	key, member1, member2 := message[4], message[6], message[8]
	unit := "m"
	if len(message) > 10 && message[10] != "" {
		unit = message[10]
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	score1, ok1 := s.sset[key][member1]
	score2, ok2 := s.sset[key][member2]
	if !ok1 || !ok2 {
		return "$-1\r\n", nil
	}

	distance := haversineDistance(decode(uint64(score1)), decode(uint64(score2)))
	distance, ok := convertDistance(distance, unit)
	if !ok {
		return fmt.Sprintf("-ERR unsupported unit provided. please use m, km, ft, mi\r\n"), nil
	}

	distanceStr := strconv.FormatFloat(distance, 'f', 4, 64)
	return fmt.Sprintf("$%d\r\n%s\r\n", len(distanceStr), distanceStr), nil
}

// handleGEOSEARCHCmd handles the GEOSEARCH redis cmd.
func (s *Server) handleGEOSEARCHCmd(client *Client, message []string) (string, error) {
	if client.queueCmds {
		client.cmdList = append(client.cmdList, message)
		return "+QUEUED\r\n", nil
	}

	if len(message) <= 16 || strings.ToUpper(message[6]) != "FROMLONLAT" || strings.ToUpper(message[12]) != "BYRADIUS" {
		return "-ERR unsupported GEOSEARCH syntax\r\n", nil
	}

	key := message[4]
	longitude, err := strconv.ParseFloat(message[8], 64)
	if err != nil {
		return fmt.Sprintf("-ERR invalid longitude,latitude pair %s,%s\r\n", message[8], message[10]), nil
	}

	latitude, err := strconv.ParseFloat(message[10], 64)
	if err != nil {
		return fmt.Sprintf("-ERR invalid longitude,latitude pair %s,%s\r\n", message[8], message[10]), nil
	}

	if longitude < MIN_LONGITUDE || longitude > MAX_LONGITUDE || latitude < MIN_LATITUDE || latitude > MAX_LATITUDE {
		return fmt.Sprintf("-ERR invalid longitude,latitude pair %f,%f\r\n", longitude, latitude), nil
	}

	radius, err := strconv.ParseFloat(message[14], 64)
	if err != nil {
		return "", err
	}

	radiusMeters, ok := distanceToMeters(radius, message[16])
	if !ok {
		return "-ERR unsupported unit provided. please use m, km, ft, mi\r\n", nil
	}

	center := Coordinates{Latitude: latitude, Longitude: longitude}

	s.mu.Lock()
	defer s.mu.Unlock()

	matches := make([]string, 0)
	for _, entry := range sortedSetEntries(s.sset[key]) {
		coordinates := decode(uint64(entry.score))
		if haversineDistance(center, coordinates) <= radiusMeters {
			matches = append(matches, entry.member)
		}
	}

	resp := fmt.Sprintf("*%d\r\n", len(matches))
	for _, member := range matches {
		resp += fmt.Sprintf("$%d\r\n%s\r\n", len(member), member)
	}

	return resp, nil
}

// ===========================
// Encoding logic for latitudes
// and longitude.
// Taken from: https://github.com/codecrafters-io/redis-geocoding-algorithm/blob/main/go/encode.go
type Coordinates struct {
	Latitude  float64
	Longitude float64
}

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

func compactInt64ToInt32(v uint64) uint32 {
	result := v & 0x5555555555555555
	result = (result | (result >> 1)) & 0x3333333333333333
	result = (result | (result >> 2)) & 0x0F0F0F0F0F0F0F0F
	result = (result | (result >> 4)) & 0x00FF00FF00FF00FF
	result = (result | (result >> 8)) & 0x0000FFFF0000FFFF
	result = (result | (result >> 16)) & 0x00000000FFFFFFFF
	return uint32(result)
}

func convertGridNumbersToCoordinates(gridLatitudeNumber, gridLongitudeNumber uint32) Coordinates {
	// Calculate the grid boundaries
	gridLatitudeMin := MIN_LATITUDE + LATITUDE_RANGE*(float64(gridLatitudeNumber)/math.Pow(2, 26))
	gridLatitudeMax := MIN_LATITUDE + LATITUDE_RANGE*(float64(gridLatitudeNumber+1)/math.Pow(2, 26))
	gridLongitudeMin := MIN_LONGITUDE + LONGITUDE_RANGE*(float64(gridLongitudeNumber)/math.Pow(2, 26))
	gridLongitudeMax := MIN_LONGITUDE + LONGITUDE_RANGE*(float64(gridLongitudeNumber+1)/math.Pow(2, 26))

	// Calculate the center point of the grid cell
	latitude := (gridLatitudeMin + gridLatitudeMax) / 2
	longitude := (gridLongitudeMin + gridLongitudeMax) / 2

	return Coordinates{Latitude: latitude, Longitude: longitude}
}

func decode(geoCode uint64) Coordinates {
	// Align bits of both latitude and longitude to take even-numbered position
	y := geoCode >> 1
	x := geoCode

	// Compact bits back to 32-bit ints
	gridLatitudeNumber := compactInt64ToInt32(x)
	gridLongitudeNumber := compactInt64ToInt32(y)

	return convertGridNumbersToCoordinates(gridLatitudeNumber, gridLongitudeNumber)
}

func degreesToRadians(degrees float64) float64 {
	return degrees * math.Pi / 180
}

func haversineDistance(from, to Coordinates) float64 {
	lat1 := degreesToRadians(from.Latitude)
	lat2 := degreesToRadians(to.Latitude)
	dLat := degreesToRadians(to.Latitude - from.Latitude)
	dLon := degreesToRadians(to.Longitude - from.Longitude)

	a := math.Pow(math.Sin(dLat/2), 2) + math.Cos(lat1)*math.Cos(lat2)*math.Pow(math.Sin(dLon/2), 2)
	c := 2 * math.Asin(math.Sqrt(a))
	return EARTH_RADIUS_METERS * c
}

func convertDistance(distance float64, unit string) (float64, bool) {
	switch unit {
	case "m":
		return distance, true
	case "km":
		return distance / 1000, true
	case "ft":
		return distance * 3.280839895, true
	case "mi":
		return distance / 1609.344, true
	default:
		return 0, false
	}
}

func distanceToMeters(distance float64, unit string) (float64, bool) {
	switch unit {
	case "m":
		return distance, true
	case "km":
		return distance * 1000, true
	case "ft":
		return distance / 3.280839895, true
	case "mi":
		return distance * 1609.344, true
	default:
		return 0, false
	}
}

// ===========================
