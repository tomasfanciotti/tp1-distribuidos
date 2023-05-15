from .rabbit_interface import RabbitInterface
from .messaging_protocol import *
from .eof import EOF
import time

from .server import ServerInterface

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level="INFO",
    datefmt='%Y-%m-%d %H:%M:%S',
)

# reduce pika log level
logging.getLogger("pika").setLevel(logging.WARNING)

# Excepcionales
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
OP_CODE_RESPONSE_QUERY2 = 9
OP_CODE_RESPONSE_QUERY3 = 10
OP_CODE_WAIT = 11
OP_CODE_ERROR = 12
OP_CODE_EOF = 13

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
                opcode, result = self.handle_querys(rabbit)
                if opcode is None or result is None:
                    send(s, Packet.new(OP_CODE_ERROR, "No seras muy fantaseosa vos?"))
                    logging.error(
                        f'action: handle_querys | result: fail | client: {addr[0]} | msg: {result}.')
                else:
                    send(s, Packet.new(opcode, result))
                    logging.info(
                        f'action: handle_querys | result: success | client: {addr[0]} | msg: {result}')
            elif packet.opcode == OP_CODE_EOF:
                data = packet.get()
                self.sendEOF(data, rabbit)
                send(s, Packet.new(OP_CODE_ACK, "joya"))

            elif packet.opcode == OP_CODE_ZERO:
                logging.info(f'action: disconnected | result: success | ip: {addr[0]}')
                break

        rabbit.disconnect()

    def sendEOF(self, archivo, rabbit: RabbitInterface):

        eof = EOF("start", "server")

        if archivo == "weathers":
            eof.channel = "raw_weather_data"
            rabbit.publish_queue("raw_weather_data", eof.encode(), headers={"original":"true"})

        elif archivo == "stations":
            eof.channel = "raw_station_data"
            rabbit.publish_queue("raw_station_data", eof.encode(), headers={"original":"true"})

        elif archivo == "trips":
            eof.channel = "raw_trip_data"
            rabbit.publish_queue("raw_trip_data", eof.encode(), headers={"original":"true"})
        else:
            return

        logging.info(f'action: sending EOF | result: success | file: {archivo}')

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

    def handle_querys(self, rabbit):

        method_frame, header_frame, body = rabbit.channel.basic_get('query1-pipe2')
        if method_frame:
            rabbit.channel.basic_ack(method_frame.delivery_tag)
            return OP_CODE_RESPONSE_QUERY1, body

        method_frame, header_frame, body = rabbit.channel.basic_get('query2-pipe2')
        if method_frame:
            rabbit.channel.basic_ack(method_frame.delivery_tag)
            return OP_CODE_RESPONSE_QUERY2, body

        method_frame, header_frame, body = rabbit.channel.basic_get('query3-pipe4')
        if method_frame:
            rabbit.channel.basic_ack(method_frame.delivery_tag)
            return OP_CODE_RESPONSE_QUERY3, body

        return OP_CODE_WAIT, "para wacha"
