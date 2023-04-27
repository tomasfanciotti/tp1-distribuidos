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
	OP_CODE_QUERY1          = 7
	OP_CODE_RESPONSE_QUERY1 = 8
	OP_CODE_ERROR           = 9
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

func (bra *BikeRidesAnalyzer) Query1() (string, error) {

	new_packet := NewPacket(OP_CODE_QUERY1, "Decime query 1 plis")
	Send(*bra.conn, new_packet)

	packet_response, err := Receive(*bra.conn)

	if err != nil {
		return "", err
	}

	if packet_response.opcode != OP_CODE_RESPONSE_QUERY1 {
		return "", errors.New("servidor NO devolvió OPCODE esperado. Obtenido: " + string(rune(packet_response.opcode)) + " - " + string(packet_response.data))
	}

	return string(packet_response.data), nil

}

/*
func (l *Lottery) getArgumentsFromBet(bet *Bet) []string {

	arguments := []string{
		bet.Agencia,
		bet.Nombre,
		bet.Apellido,
		bet.Dni,
		bet.Nacimiento,
		bet.Numero,
	}

	return arguments
}

func (l *Lottery) almacenar_apuesta(bet *Bet, client_id string) (bool, error) {

	arguments := l.getArgumentsFromBet(bet)
	new_packet := NewPacket(OP_CODE_REGISTER, arguments)
	Send(*l.conn, new_packet)

	packet_response, err := Receive(*l.conn)

	if err != nil {
		return false, err
	}

	if packet_response.opcode != OP_CODE_REGISTER_ACK {
		return false, errors.New("Servidor NO devolvió ACK")
	}

	return true, nil
}

func (l *Lottery) almacenar_bacth(batch []Bet) (bool, error) {

	arguments := []string{strconv.Itoa(len(batch))}
	for i, v := range batch {
		arguments = append(arguments, "@"+strconv.Itoa(i))
		arguments = append(arguments, l.getArgumentsFromBet(&v)...)
	}

	new_packet := NewPacket(OP_CODE_REGISTER_BATCH, arguments)
	Send(*l.conn, new_packet)

	packet_response, err := Receive(*l.conn)

	if err != nil {
		return false, err
	}

	if packet_response.opcode != OP_CODE_REGISTER_ACK {
		return false, errors.New("Servidor NO devolvió ACK")
	}

	return true, nil
}

func (l *Lottery) ready(client_id string) (bool, error) {

	arguments := []string{client_id}
	packet := NewPacket(OP_CODE_AGENCY_READY, arguments)
	Send(*l.conn, packet)
	response, err := Receive(*l.conn)
	if err != nil {
		return false, err
	}

	switch response.opcode {
	case OP_CODE_ZERO:
		return false, errors.New("Conexión cerrada")

	case OP_CODE_ERROR:
		return false, errors.New(string(response.data))

	case OP_CODE_REGISTER_ACK:
		return true, nil

	default:
		return false, errors.New("unexpected response code: " + string(response.opcode))
	}
}

func (l *Lottery) winner(agency_id string) ([]string, error) {

	arguments := []string{agency_id}
	ask_winner := NewPacket(OP_CODE_ASK_WINNER, arguments)
	Send(*l.conn, ask_winner)
	response, err := Receive(*l.conn)
	if err != nil {
		return nil, err
	}

	// Conexion cerrada
	if response.opcode == OP_CODE_ZERO {
		return nil, errors.New("Conexión cerrada")
	}

	// Error Server
	if response.opcode == OP_CODE_ERROR {
		return nil, errors.New(string(response.data))
	}

	// Servidor ocupado
	if response.opcode == OP_CODE_SERVER_BUSSY {
		return nil, nil
	}

	// Otro OpCode
	if response.opcode != OP_CODE_WINNERS {
		return nil, errors.New("unexpected opcode: " + string(response.opcode))
	}

	// Winners
	winners, ok := decode(response.data).([]string)
	if !ok {
		return nil, errors.New("unexpected data: " + string(response.data))
	}

	return winners, nil
}

*/
