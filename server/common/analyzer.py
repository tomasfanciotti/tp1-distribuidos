from .rabbit_interface import *
from .messaging_protocol import *
from .eof import EOF
import time

# Excepcionales
from .server import ServerInterface

OP_CODE_CORRUPTED_REQUEST = -1
OP_CODE_ZERO = 0

# OpCodes
OP_CODE_PING = 1
OP_CODE_PONG = 2
OP_CODE_INGEST_WEATHER = 3
OP_CODE_INGEST_STATIONS = 4
OP_CODE_INGEST_TRIPS = 5
OP_CODE_ACK = 6
OP_CODE_QUERY1 = 7
OP_CODE_RESPONSE_QUERY1 = 8
OP_CODE_ERROR = 9
OP_CODE_EOF = 10

# Wait for rabbitmq to come up
time.sleep(10)

WEATHER_FIELDS = 22
STATION_FIELDS = 7
TRIPS_FIELDS = 9



class Analyzer(ServerInterface):

    def handle_client(self, s):

        rabbit = RabbitInterface()

        while True:
            packet = receive(s)
            addr = s.getpeername()
            logging.debug(
                f'action: receive_message | result: success | ip: {addr[0]} | msg: {packet.get()}')

            if packet.opcode == OP_CODE_PING:

                send(s, Packet.new(OP_CODE_PONG, "Pong!"))
                logging.info(f'action: ping | result: success | client: {addr[0]} | msg: Ponged successfully.')

            elif packet.opcode == OP_CODE_INGEST_WEATHER:

                data = packet.get()
                result = self.handle_wheather(data, rabbit)
                if not result:
                    send(s, Packet.new(OP_CODE_ERROR, "There was a problem handling weathers"))
                    logging.error(
                        f'action: ingest_weather | result: fail | client: {addr[0]} | msg: Error in the ingestion of weathers.')
                else:
                    send(s, Packet.new(OP_CODE_ACK, "ACK!"))
                    logging.info(
                        f'action: ingest_weather | result: success | client: {addr[0]} | msg: Weathers ingested correctly.')

            elif packet.opcode == OP_CODE_INGEST_STATIONS:

                data = packet.get()
                result = self.handle_stations(data, rabbit)
                if not result:
                    send(s, Packet.new(OP_CODE_ERROR, "There was a problem handling stations"))
                    logging.error(
                        f'action: ingest_stations | result: fail | client: {addr[0]} | msg: Error in the ingestion of stations.')
                else:
                    send(s, Packet.new(OP_CODE_ACK, "ACK!"))
                    logging.info(
                        f'action: ingest_stations | result: success | client: {addr[0]} | msg: Stations ingested correctly.')

            elif packet.opcode == OP_CODE_INGEST_TRIPS:

                data = packet.get()
                result = self.handle_trips(data, rabbit)
                if not result:
                    send(s, Packet.new(OP_CODE_ERROR, "There was a problem handling trips"))
                    logging.error(
                        f'action: ingest_trips | result: fail | client: {addr[0]} | msg: Error in the ingestion of trips.')
                else:
                    send(s, Packet.new(OP_CODE_ACK, "ACK!"))
                    logging.info(
                        f'action: ingest_strips | result: success | client: {addr[0]} | msg: trips ingested correctly.')

            elif packet.opcode == OP_CODE_QUERY1:

                data = packet.get()
                result = self.handle_query_1()
                if result is None:
                    send(s, Packet.new(OP_CODE_ERROR, "No seras muy fantaseosa vos?"))
                    logging.error(
                        f'action: query #1 | result: fail | client: {addr[0]} | msg: Error in the retreival of query #1.')
                else:
                    send(s, Packet.new(OP_CODE_RESPONSE_QUERY1, result))
                    logging.info(
                        f'action: query #1 | result: success | client: {addr[0]} | msg: query #1 retreived correctly.')
            elif packet.opcode == OP_CODE_EOF:
                data = packet.get()
                self.sendEOF(data, rabbit)
                send(s, Packet.new(OP_CODE_ACK, "joya"))

            elif packet.opcode == OP_CODE_ZERO:
                logging.info(f'action: disconnected | result: success | ip: {addr[0]}')
                break

        rabbit.disconnect()

    def sendEOF(self, archivo, rabbit: RabbitInterface):

        eof = EOF("start","server").encode()
        rabbit.publish_queue("raw_weather_data", eof, headers={"original":"true"})
        rabbit.publish_queue("raw_station_data", eof, headers={"original":"true"})
        rabbit.publish_queue("raw_trip_data", eof, headers={"original":"true"})

    def handle_wheather(self, data, rabbit: RabbitInterface):

        # Cantidad de weathers
        batch_size = int(data.pop(0))

        try:
            for i in range(batch_size):
                reg = data[i * WEATHER_FIELDS + 1:(i + 1) * WEATHER_FIELDS]
                rabbit.publish_queue("raw_weather_data", encode(reg))

        except Exception as e:
            logging.error(
                f'action: analyzer_handle_wheather | result: fail | msg: {e}')
            return False

        return True

    def handle_stations(self, data, rabbit: RabbitInterface):

        batch_size = int(data.pop(0))

        try:

            for i in range(batch_size):
                reg = data[i * STATION_FIELDS + 1:(i + 1) * STATION_FIELDS]
                rabbit.publish_queue("raw_station_data", encode(reg))

        except Exception as e:
            logging.error(
                f'action: analyzer_handle_stations | result: fail | msg: {e}')
            return False

        return True

    def handle_trips(self, data, rabbit: RabbitInterface):

        batch_size = int(data.pop(0))

        try:

            for i in range(batch_size):
                reg = data[i * TRIPS_FIELDS + 1:(i + 1) * TRIPS_FIELDS]
                rabbit.publish_queue("raw_trip_data", encode(reg))

        except Exception as e:
            logging.error(
                f'action: analyzer_handle_stations | result: fail | msg: {e}')
            return False

        return True

    def handle_query_1(self):
        return None
