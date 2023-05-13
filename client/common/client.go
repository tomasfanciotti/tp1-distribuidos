package common

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	WAITING_PERIOD   = 4
	MAX_BATCH_SIZE   = 1000
	WEATHER_FILE     = "weather.csv"
	STATION_FILE     = "station.csv"
	TRIP_FILE        = "trip.csv"
	FINISH_MESSAGE   = "FIN"
	EXPECTED_QUERIES = 3
)

// ClientConfig Configuration used by the client
type ClientConfig struct {
	ID            string
	ServerAddress string
	LoopLapse     time.Duration
	LoopPeriod    time.Duration
	DataSource    string
}

// Client Entity that encapsulates how
type Client struct {
	config   ClientConfig
	conn     net.Conn
	analyzer *BikeRidesAnalyzer
	running  bool
}

// NewClient Initializes a new client receiving the configuration
// as a parameter
func NewClient(config ClientConfig) *Client {
	client := &Client{
		config:  config,
		running: false,
	}
	return client
}

// CreateClientSocket Initializes client socket. In case of
// failure, error is printed in stdout/stderr and exit 1
// is returned
func (c *Client) createClientSocket() error {
	conn, err := net.Dial("tcp", c.config.ServerAddress)
	if err != nil {
		log.Fatalf(
			"action: connect | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
	}
	c.conn = conn
	return nil
}

type BatchUnitData struct {
	batchType string
	data      []string
}

func readFoldercontent(DataFolder string) ([]string, error) {

	dir, err := os.Open(DataFolder)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	defer dir.Close()

	fileInfos, err := dir.Readdir(-1)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	result := []string{}
	for _, fileInfo := range fileInfos {

		if fileInfo.IsDir() && fileInfo.Name() != ".DS_STORE" {
			result = append(result, fileInfo.Name())
		}

	}

	return result, nil
}

func batchDataProcessor(c *Client, fileName string, handler func(*Client, []BatchUnitData) bool, expected_fields int, batchType string, city string) (int, bool) {

	file, err := os.Open(fileName)
	if err != nil {
		log.Errorf("action: open_batch_file | result: fail | err: %s", err)
		return 0, false
	}
	defer file.Close()

	log.Infof("action: batch_data_processor | result: in_progress | msg: Archivo: %v", fileName)
	scanner := bufio.NewScanner(file)
	batch := []BatchUnitData{}
	total_inputs := 0
	result := true

	for scanner.Scan() {

		if !c.running {
			log.Infof("action: scan_batch_file | result: error | msg: client is shutting down")
			return 0, false
		}

		campos := strings.Split(strings.TrimRight(scanner.Text(), "\n"), ",")

		if len(campos) != expected_fields {
			log.Infof("action: scan_batch_file | result: warning | msg: line fields does not match with a expected register. Found: %v in { %v }  ignoring..", len(campos), campos)
			continue
		}

		campos = append([]string{city}, campos...)
		data := BatchUnitData{batchType: batchType, data: campos}
		batch = append(batch, data)

		if len(batch) >= MAX_BATCH_SIZE {

			result = result && handler(c, batch)
			total_inputs += 1
			batch = []BatchUnitData{}
		}
	}

	if len(batch) > 0 {
		result = result && handler(c, batch)
		total_inputs += 1
	}

	return total_inputs, result
}

func (c *Client) SetupGracefulShutdown() {

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		c.conn.Close()
		c.running = false
		log.Errorf("action: receive_message | result: fail | client_id: %v ", c.config.ID)
	}()
}

func ingestWeatherHandler(c *Client, batch []BatchUnitData) bool {

	_, err := c.analyzer.IngestWeather(batch)
	if err != nil {
		log.Errorf("action: ingest_weather_batch | result: fail | err: %s", err)
		return false
	}

	log.Infof("action: ingest_weather_handler | result: success | client_id: %v  | msg: Batch de %v weathers.", c.config.ID, len(batch))
	return true
}

func ingestStationHandler(c *Client, batch []BatchUnitData) bool {

	_, err := c.analyzer.IngestStation(batch)
	if err != nil {
		log.Errorf("action: ingest_station_batch | result: fail | err: %s", err)
		return false
	}

	log.Infof("action: ingest_station_handler | result: success | client_id: %v  | msg: Batch de %v stations.", c.config.ID, len(batch))
	return true
}

func ingestTripsHandler(c *Client, batch []BatchUnitData) bool {

	_, err := c.analyzer.IngestTrip(batch)
	if err != nil {
		log.Errorf("action: ingest_trip_batch | result: fail | err: %s", err)
		return false
	}

	log.Infof("action: ingest_trip_handler | result: success | client_id: %v  | msg: Batch de %v trips.", c.config.ID, len(batch))
	return true
}

// StartClientLoop Send messages to the client until some time threshold is met
func (c *Client) StartClientLoop() {

	c.running = true

loop:
	// Send messages if the loopLapse threshold has not been surpassed
	for timeout := time.After(c.config.LoopLapse); ; {
		select {
		case <-timeout:
			log.Infof("action: timeout_detected | result: success | client_id: %v",
				c.config.ID,
			)
			break loop
		default:
			if !c.running {
				break
			}
		}

		c.createClientSocket()

		// Request something to the server interface

		c.conn.Close()

		// Wait a time between sending one message and the next one
		time.Sleep(c.config.LoopPeriod)
	}

	log.Infof("action: loop_finished | result: success | client_id: %v", c.config.ID)
}

func (c *Client) Start() {

	c.createClientSocket()

	c.analyzer = NewAnalyzer(&c.conn)
	c.running = true

	// Ruta de la carpeta principal que contiene las ciudades
	root := c.config.DataSource
	cities, err := readFoldercontent(root)
	if err != nil {
		log.Errorf("action: get_files | result: fail | root: %v | err: %v", root, err)
		c.running = false
		return
	}

	log.Infof("action: get_files | result: success | cities: %v", cities)

	// Send Weather
	for _, city := range cities {

		registers, result := batchDataProcessor(c, root+city+"/"+WEATHER_FILE, ingestWeatherHandler, 20, "WEATHER", city)

		if !result {
			log.Warnf("action: send_weather | result: warning | client_id: %v | msg: some weather batch could not be send in city %v", c.config.ID, city)
		}

		log.Infof("action: send_weather | result: success | client_id: %v | msg: sent %v weather batchs of city %v.", c.config.ID, registers, city)
	}

	c.analyzer.SendEOF("weathers")

	// Send Stations
	for _, city := range cities {

		registers, result := batchDataProcessor(c, root+city+"/"+STATION_FILE, ingestStationHandler, 5, "STATION", city)

		if !result {
			log.Warnf("action: send_stations | result: warning | client_id: %v | msg: some station batch could not be send", c.config.ID)
		}
		log.Infof("action: send_stations | result: success | client_id: %v | msg: sent %v station batchs.", c.config.ID, registers)
	}

	c.analyzer.SendEOF("stations")

	// Send Trips
	for _, city := range cities {

		registers, result := batchDataProcessor(c, root+city+"/"+TRIP_FILE, ingestTripsHandler, 7, "TRIP", city)

		if !result {
			log.Warnf("action: send_trips | result: warning | client_id: %v | msg: some trips batch could not be send", c.config.ID)
		}
		log.Infof("action: send_trips | result: success | client_id: %v | msg: sent %v trips batchs.", c.config.ID, registers)
	}

	c.analyzer.SendEOF("trips")

	responses := 0
	for responses < EXPECTED_QUERIES && c.running {

		result, err := c.analyzer.GetQueries()
		if err != nil {
			log.Errorf("action: query_request | result: fail | client_id: %v | msg: %v", c.config.ID, err)
			break

		} else if result == FINISH_MESSAGE {
			log.Infof("action: query_request | result: finish ")
			break

		} else if result != "" {
			log.Infof("action: query_request | result: success | msg: %v", result)
			responses += 1

		} else {
			log.Infof("action: query_request | result: fail | msg: no queries ready, waiting..")
			time.Sleep(WAITING_PERIOD * time.Second)
		}
	}

	c.conn.Close()

	// End
	log.Infof("action: main | result: success | client_id: %v", c.config.ID)

}
