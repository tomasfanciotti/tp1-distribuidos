package common

import (
	"errors"
	"net"
	"strconv"
)

// OpCodes
const (
	OP_CODE_ZERO            = 0
	OP_CODE_PING            = 1
	OP_CODE_PONG            = 2
	OP_CODE_INGEST_WEATHER  = 3
	OP_CODE_INGEST_STATIONS = 4
	OP_CODE_INGEST_TRIPS    = 5
	OP_CODE_ACK             = 6
	OP_CODE_QUERYES         = 7
	OP_CODE_RESPONSE_QUERY1 = 8
	OP_CODE_RESPONSE_QUERY2 = 9
	OP_CODE_RESPONSE_QUERY3 = 10
	OP_CODE_FINISH          = 11
	OP_CODE_ERROR           = 12
	OP_CODE_EOF             = 13
)

// Bike Rides Analyzer Interface

type BikeRidesAnalyzer struct {
	conn *net.Conn
}

func NewAnalyzer(conn *net.Conn) *BikeRidesAnalyzer {
	analyzer := &BikeRidesAnalyzer{
		conn: conn,
	}
	return analyzer
}

func (bra *BikeRidesAnalyzer) Ping() (bool, error) {

	new_packet := NewPacket(OP_CODE_PING, nil)
	Send(*bra.conn, new_packet)

	packet_response, err := Receive(*bra.conn)

	if err != nil {
		return false, err
	}

	if packet_response.opcode != OP_CODE_PONG {
		return false, errors.New("Servidor no respondió PONG. OPCODE recivido " + string(rune(packet_response.opcode)))
	}

	return true, nil
}

func (bra *BikeRidesAnalyzer) IngestWeather(data []BatchUnitData) (bool, error) {

	arguments := []string{strconv.Itoa(len(data))}
	for i, v := range data {

		if v.batchType != "WEATHER" {
			return false, errors.New("Tipo de bacth invalido: " + v.batchType)
		}

		arguments = append(arguments, "@"+strconv.Itoa(i))
		arguments = append(arguments, v.data...)
	}

	new_packet := NewPacket(OP_CODE_INGEST_WEATHER, arguments)
	Send(*bra.conn, new_packet)

	packet_response, err := Receive(*bra.conn)

	if err != nil {
		return false, err
	}

	if packet_response.opcode != OP_CODE_ACK {
		return false, errors.New("servidor NO devolvió ACK")
	}

	return true, nil

}

func (bra *BikeRidesAnalyzer) IngestStation(data []BatchUnitData) (bool, error) {

	arguments := []string{strconv.Itoa(len(data))}
	for i, v := range data {

		if v.batchType != "STATION" {
			return false, errors.New("Tipo de bacth invalido: " + v.batchType)
		}

		arguments = append(arguments, "@"+strconv.Itoa(i))
		arguments = append(arguments, v.data...)
	}

	new_packet := NewPacket(OP_CODE_INGEST_STATIONS, arguments)
	Send(*bra.conn, new_packet)

	packet_response, err := Receive(*bra.conn)

	if err != nil {
		return false, err
	}

	if packet_response.opcode != OP_CODE_ACK {
		return false, errors.New("servidor NO devolvió ACK")
	}

	return true, nil

}

func (bra *BikeRidesAnalyzer) IngestTrip(data []BatchUnitData) (bool, error) {

	arguments := []string{strconv.Itoa(len(data))}
	for i, v := range data {

		if v.batchType != "TRIP" {
			return false, errors.New("Tipo de bacth invalido: " + v.batchType)
		}

		arguments = append(arguments, "@"+strconv.Itoa(i))
		arguments = append(arguments, v.data...)
	}

	new_packet := NewPacket(OP_CODE_INGEST_TRIPS, arguments)
	Send(*bra.conn, new_packet)

	packet_response, err := Receive(*bra.conn)

	if err != nil {
		return false, err
	}

	if packet_response.opcode != OP_CODE_ACK {
		return false, errors.New("servidor NO devolvió ACK")
	}

	return true, nil

}

func (bra *BikeRidesAnalyzer) GetQueries() (string, error) {

	new_packet := NewPacket(OP_CODE_QUERYES, "Decime alguna query pliss")
	Send(*bra.conn, new_packet)

	packet_response, err := Receive(*bra.conn)

	if err != nil {
		return "", err
	}

	if packet_response.opcode == OP_CODE_RESPONSE_QUERY1 {
		return "Query 1: " + string(packet_response.data), nil

	} else if packet_response.opcode == OP_CODE_RESPONSE_QUERY2 {
		return "Query 2: " + string(packet_response.data), nil

	} else if packet_response.opcode == OP_CODE_RESPONSE_QUERY3 {
		return "Query 3: " + string(packet_response.data), nil

	} else if packet_response.opcode == OP_CODE_FINISH {
		return "", nil
	}

	return "", errors.New("opcode does not recognized: " + string(rune(packet_response.opcode)))

}

func (bra *BikeRidesAnalyzer) SendEOF(file string) (string, error) {

	new_packet := NewPacket(OP_CODE_EOF, file)
	Send(*bra.conn, new_packet)

	packet_response, err := Receive(*bra.conn)
	if err != nil {
		return "", err
	}

	if packet_response.opcode != OP_CODE_ACK {
		return "", errors.New("servidor NO devolvió OPCODE esperado. Obtenido: " + string(rune(packet_response.opcode)) + " - " + string(packet_response.data))
	}

	return string(packet_response.data), nil
}
