package common

import (
	"bufio"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	WAITING_PERIOD = 4
	MAX_BATCH_SIZE = 20
)

// ClientConfig Configuration used by the client
type ClientConfig struct {
	ID            string
	ServerAddress string
	LoopLapse     time.Duration
	LoopPeriod    time.Duration
	DataPath      string
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

func batchDataProcessor(c *Client, fileName string, handler func(*Client, []BatchUnitData) bool, expected_fields int, batchType string) (int, bool) {

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

		campos := strings.Split(strings.TrimRight(scanner.Text(), "\n"), ",")

		if len(campos) != expected_fields {
			log.Infof("action: scan_batch_file | result: warning | msg: line fields does not match with a expected register. Found: %v in { %v }  ignoring..", len(campos), campos)
			continue
		}

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

	sigChan := make(chan os.Signal)
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
		log.Errorf("action: send_batch | result: fail | err: %s", err)
		return false
	}

	log.Infof("action: ingest_weather | result: success | client_id: %v  | msg: Batch de %v ", c.config.ID, len(batch))
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

	registers, result := batchDataProcessor(c, c.config.DataPath, ingestWeatherHandler, 20, "WEATHER")

	if !result {
		log.Warnf("action: send_weather | result: warning | client_id: %v | msg: some weather batch could not be send", c.config.ID)
	}

	log.Infof("action: send_weather | result: success | client_id: %v | msg: sent %v weather registers.", c.config.ID, registers)

	log.Infof("action: execution | result: success | client_id: %v", c.config.ID)

}
